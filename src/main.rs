use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use rumqttc::{Client, Connection, Event, LastWill, MqttOptions, Packet, QoS};
use rumqttd::{Broker, ConnectionSettings, RouterConfig, ServerSettings};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};
use tiny_http::{Header, Method, Request, Response, Server};

const NEW: &str = "pizza/order/new";
const STATUS_ROOT: &str = "pizza/order/status";
const STATUS_FILTER: &str = "pizza/order/status/+";
const CLIENTS_ROOT: &str = "pizza/system/client-status";
const CLIENTS_FILTER: &str = "pizza/system/client-status/+";

const PIZZAS: [&str; 3] = ["Margherita", "Salami", "Funghi"];
const SIZES: [&str; 3] = ["Small", "Medium", "Large"];

/// Seconds after which the kitchen moves an order into the next state.
const PIPELINE: [(u64, &str); 2] = [(4, "baking"), (7, "ready")];

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    timestamp: String,
    order_id: u32,
    pizza: String,
    size: String,
    status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientStatus {
    client: String,
    status: String,
}

type Orders = Arc<Mutex<Vec<Order>>>;
type Clients = Arc<Mutex<BTreeMap<String, String>>>;
type ClientHandle = Arc<Mutex<Client>>;

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn broker() -> (String, u16) {
    let host = env::var("MQTT_BROKER").unwrap_or_else(|_| "localhost".into());
    let port = env::var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);
    (host, port)
}

fn status_topic(order_id: u32) -> String {
    format!("{STATUS_ROOT}/{order_id}")
}

fn connect(name: &str) -> (Client, Connection) {
    let (host, port) = broker();
    let mut options = MqttOptions::new(name, host, port);
    options.set_keep_alive(Duration::from_secs(5));
    options.set_clean_session(true);
    let will = serde_json::to_vec(&ClientStatus {
        client: name.into(),
        status: "offline".into(),
    })
    .expect("ClientStatus is always serializable");
    options.set_last_will(LastWill::new(
        format!("{CLIENTS_ROOT}/{name}"),
        will,
        QoS::AtLeastOnce,
        true,
    ));
    Client::new(options, 20)
}

fn publish_json<T: Serialize>(
    client: &mut Client,
    topic: &str,
    value: &T,
    qos: QoS,
    retain: bool,
) -> Result<()> {
    client.publish(topic, qos, retain, serde_json::to_vec(value)?)?;
    Ok(())
}

fn online(client: &mut Client, name: &str) -> Result<()> {
    publish_json(
        client,
        &format!("{CLIENTS_ROOT}/{name}"),
        &ClientStatus {
            client: name.into(),
            status: "online".into(),
        },
        QoS::AtLeastOnce,
        true,
    )
}

/// Runs the event loop forever. rumqttc reconnects on its own, so an error here
/// is logged and the iteration continues.
fn pump<F>(mut connection: Connection, mut on_publish: F)
where
    F: FnMut(&str, &[u8]) + Send + 'static,
{
    thread::spawn(move || {
        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Packet::Publish(packet))) => {
                    on_publish(&packet.topic, &packet.payload)
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("mqtt: {error}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
}

fn upsert(orders: &Orders, order: Order) {
    let mut list = orders.lock().unwrap();
    match list.iter_mut().find(|x| x.order_id == order.order_id) {
        Some(existing) => *existing = order,
        None => list.push(order),
    }
}

// ---------------------------------------------------------------- http layer

enum Reply {
    Html(String),
    Json(String),
    Redirect(&'static str),
}

fn form_field(form: &str, key: &str) -> Option<String> {
    form.split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| {
            urlencoding::decode(&v.replace('+', " "))
                .unwrap_or_default()
                .into_owned()
        })
}

fn pick(form: &str, key: &str, allowed: &[&str]) -> Result<String> {
    let value = form_field(form, key).unwrap_or_default();
    match allowed.iter().find(|a| **a == value) {
        Some(found) => Ok((*found).to_string()),
        None => bail!("invalid {key}: {value:?}"),
    }
}

fn respond(request: Request, reply: Reply) {
    let result = match reply {
        Reply::Html(body) => {
            let header = Header::from_bytes("Content-Type", "text/html; charset=utf-8").unwrap();
            request.respond(Response::from_string(body).with_header(header))
        }
        Reply::Json(body) => {
            let header = Header::from_bytes("Content-Type", "application/json").unwrap();
            request.respond(Response::from_string(body).with_header(header))
        }
        Reply::Redirect(target) => {
            let header = Header::from_bytes("Location", target).unwrap();
            request.respond(Response::empty(303).with_header(header))
        }
    };
    if let Err(error) = result {
        eprintln!("http: {error}");
    }
}

fn http(addr: &str) -> Result<Server> {
    Server::http(addr).map_err(|error| anyhow!("bind {addr}: {error}"))
}

fn serve<H>(server: Server, handle: H)
where
    H: Fn(&Method, &str, String) -> Reply,
{
    for mut request in server.incoming_requests() {
        let method = request.method().clone();
        let path = request.url().split('?').next().unwrap_or("/").to_string();
        let mut body = String::new();
        if method == Method::Post {
            let _ = request.as_reader().read_to_string(&mut body);
        }
        let reply = handle(&method, &path, body);
        respond(request, reply);
    }
}

// -------------------------------------------------------------------- markup

const CSS: &str = r#"
*{box-sizing:border-box}
:root{
  --bg:#100e0c; --panel:#191612; --line:#2c261f; --ink:#f2ece3; --muted:#9b9086;
  --ember:#e2662a; --ok:#68a96b; --warm:#d9a441; --mono:ui-monospace,"SF Mono",Menlo,monospace;
}
body{margin:0;background:var(--bg);color:var(--ink);
  font:16px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;
  -webkit-font-smoothing:antialiased}
.wrap{max-width:860px;margin:0 auto;padding:44px 22px 80px}
header{display:flex;align-items:baseline;justify-content:space-between;gap:18px;
  padding-bottom:18px;border-bottom:1px solid var(--line);flex-wrap:wrap}
h1{margin:0;font-size:27px;letter-spacing:-.02em;font-weight:620}
h1 .dot{color:var(--ember)}
.meta{font-family:var(--mono);font-size:12px;color:var(--muted);letter-spacing:.02em}
h2{margin:0 0 14px;font-size:13px;font-weight:600;letter-spacing:.14em;
  text-transform:uppercase;color:var(--muted)}
section{margin-top:34px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:20px}
form{display:flex;gap:10px;flex-wrap:wrap;align-items:stretch}
select,button{font:inherit;border-radius:7px;border:1px solid var(--line);padding:11px 14px}
select{background:#221d18;color:var(--ink);min-width:150px;flex:1}
select:focus,button:focus{outline:2px solid var(--ember);outline-offset:2px}
button{background:var(--ember);border-color:var(--ember);color:#160f09;font-weight:640;
  cursor:pointer;letter-spacing:.01em}
button:hover{filter:brightness(1.08)}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;letter-spacing:.12em;text-transform:uppercase;
  color:var(--muted);font-weight:600;padding:0 0 10px}
td{padding:12px 0;border-top:1px solid var(--line);vertical-align:middle}
td.id{font-family:var(--mono);color:var(--muted);width:78px}
td.time{font-family:var(--mono);font-size:12px;color:var(--muted);text-align:right}
.pill{display:inline-block;font-family:var(--mono);font-size:11px;letter-spacing:.06em;
  text-transform:uppercase;padding:4px 9px;border-radius:999px;
  border:1px solid currentColor;color:var(--muted)}
.pill.ordered{color:var(--muted)}
.pill.baking{color:var(--warm)}
.pill.ready{color:var(--ok)}
.pill.online{color:var(--ok)}
.pill.offline{color:var(--ember)}
.empty{color:var(--muted);font-size:14px;padding:14px 0}
.live{display:inline-flex;align-items:center;gap:7px}
.live i{width:7px;height:7px;border-radius:50%;background:var(--ok);
  animation:pulse 2s ease-in-out infinite}
@keyframes pulse{50%{opacity:.25}}
@media(max-width:560px){.wrap{padding:28px 16px 60px}select{min-width:0}form{flex-direction:column}}
"#;

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn page(title: &str, body: &str, script: &str) -> String {
    let (host, port) = broker();
    format!(
        r#"<!doctype html><html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} · Pizza MQTT</title><style>{CSS}</style></head>
<body><div class="wrap"><header>
<h1>{title}<span class="dot">.</span></h1>
<span class="meta live"><i></i>{host}:{port}</span>
</header>{body}</div><script>{script}</script></body></html>"#,
        title = escape(title),
        CSS = CSS,
        host = escape(&host),
        body = body,
        script = script,
    )
}

/// Shared client-side helpers: pill markup, relative timestamps, polling.
const JS_LIB: &str = r#"
const pill=s=>'<span class="pill '+s+'">'+s+'</span>';
const ago=t=>{const d=(Date.now()-new Date(t))/1000;
  return d<60?Math.max(0,d|0)+'s':d<3600?(d/60|0)+'m':(d/3600|0)+'h'};
const poll=(url,draw)=>{const tick=()=>fetch(url).then(r=>r.json()).then(draw).catch(()=>{});
  tick();setInterval(tick,1500)};
const rows=(list,cells)=>list.length
  ? '<table>'+cells.head+'<tbody>'+list.map(cells.row).join('')+'</tbody></table>'
  : '<p class="empty">'+cells.empty+'</p>';
"#;

fn order_rows_js(container: &str) -> String {
    format!(
        r#"{JS_LIB}
poll('/api',d=>{{document.getElementById('{container}').innerHTML=rows(d.orders,{{
  head:'<thead><tr><th>Nr.</th><th>Pizza</th><th>Größe</th><th>Status</th><th></th></tr></thead>',
  row:o=>'<tr><td class="id">#'+o.order_id+'</td><td>'+o.pizza+'</td><td>'+o.size+'</td><td>'
    +pill(o.status)+'</td><td class="time">'+ago(o.timestamp)+'</td></tr>',
  empty:'Noch keine Bestellung.'}})}});"#
    )
}

// ------------------------------------------------------------------ customer

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

fn next_order_id() -> u32 {
    // Timestamp-derived base keeps ids unique across restarts of the app.
    let seed = (Utc::now().timestamp() as u32).wrapping_mul(10) % 900_000 + 100_000;
    seed.wrapping_add(NEXT_ID.fetch_add(1, Ordering::Relaxed)) % 1_000_000
}

fn customer() -> Result<()> {
    let (client, connection) = connect("customer");
    let client: ClientHandle = Arc::new(Mutex::new(client));
    online(&mut client.lock().unwrap(), "customer")?;
    client
        .lock()
        .unwrap()
        .subscribe(STATUS_FILTER, QoS::AtLeastOnce)?;

    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let received = orders.clone();
    pump(connection, move |topic, payload| {
        if topic.starts_with(STATUS_ROOT)
            && let Ok(order) = serde_json::from_slice::<Order>(payload)
        {
            upsert(&received, order);
        }
    });

    let server = http("0.0.0.0:3000")?;
    println!("customer  http://localhost:3000");
    serve(server, move |method, path, body| {
        if method == &Method::Post {
            return match place_order(&client, &body) {
                Ok(id) => {
                    println!("order #{id} placed");
                    Reply::Redirect("/")
                }
                Err(error) => {
                    eprintln!("order rejected: {error}");
                    Reply::Redirect("/")
                }
            };
        }
        if path == "/api" {
            let list = orders.lock().unwrap();
            return Reply::Json(serde_json::json!({ "orders": *list }).to_string());
        }
        let options = |values: &[&str]| -> String {
            values
                .iter()
                .map(|v| format!("<option>{v}</option>"))
                .collect()
        };
        Reply::Html(page(
            "Bestellen",
            &format!(
                r#"<section><h2>Neue Bestellung</h2><div class="panel"><form method="post" action="/">
<select name="pizza" aria-label="Pizza">{pizzas}</select>
<select name="size" aria-label="Größe">{sizes}</select>
<button>Bestellen</button></form></div></section>
<section><h2>Meine Bestellungen</h2><div class="panel" id="orders"></div></section>"#,
                pizzas = options(&PIZZAS),
                sizes = options(&SIZES),
            ),
            &order_rows_js("orders"),
        ))
    });
    Ok(())
}

fn place_order(client: &ClientHandle, form: &str) -> Result<u32> {
    let pizza = pick(form, "pizza", &PIZZAS)?;
    let size = pick(form, "size", &SIZES)?;
    let order = Order {
        timestamp: now(),
        order_id: next_order_id(),
        pizza,
        size,
        status: "ordered".into(),
    };
    publish_json(
        &mut client.lock().unwrap(),
        NEW,
        &order,
        QoS::AtLeastOnce,
        false,
    )?;
    Ok(order.order_id)
}

// ------------------------------------------------------------------- kitchen

fn kitchen() -> Result<()> {
    let (client, connection) = connect("kitchen");
    let client: ClientHandle = Arc::new(Mutex::new(client));
    online(&mut client.lock().unwrap(), "kitchen")?;
    client.lock().unwrap().subscribe(NEW, QoS::AtLeastOnce)?;

    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let received = orders.clone();
    let sender = client.clone();
    pump(connection, move |topic, payload| {
        if topic != NEW {
            return;
        }
        let Ok(order) = serde_json::from_slice::<Order>(payload) else {
            eprintln!("kitchen: undecodable order on {topic}");
            return;
        };
        if received
            .lock()
            .unwrap()
            .iter()
            .any(|x| x.order_id == order.order_id)
        {
            return; // QoS 1 can redeliver
        }
        upsert(&received, order.clone());
        publish_status(&sender, &order);
        let cook_client = sender.clone();
        let cook_orders = received.clone();
        thread::spawn(move || cook(cook_client, cook_orders, order));
    });

    let server = http("0.0.0.0:3001")?;
    println!("kitchen   http://localhost:3001");
    serve(server, move |_, path, _| {
        if path == "/api" {
            let list = orders.lock().unwrap();
            return Reply::Json(serde_json::json!({ "orders": *list }).to_string());
        }
        Reply::Html(page(
            "Küche",
            r#"<section><h2>Eingehende Bestellungen</h2><div class="panel" id="orders"></div></section>"#,
            &order_rows_js("orders"),
        ))
    });
    Ok(())
}

fn publish_status(client: &ClientHandle, order: &Order) {
    if let Err(error) = publish_json(
        &mut client.lock().unwrap(),
        &status_topic(order.order_id),
        order,
        QoS::AtLeastOnce,
        true,
    ) {
        eprintln!("kitchen: publish failed: {error}");
    }
}

fn cook(client: ClientHandle, orders: Orders, mut order: Order) {
    for (delay, status) in PIPELINE {
        thread::sleep(Duration::from_secs(delay));
        order.status = status.into();
        order.timestamp = now();
        upsert(&orders, order.clone());
        publish_status(&client, &order);
    }
}

// ----------------------------------------------------------------- dashboard

fn dashboard() -> Result<()> {
    let (mut client, connection) = connect("dashboard");
    online(&mut client, "dashboard")?;
    client.subscribe(STATUS_FILTER, QoS::AtLeastOnce)?;
    client.subscribe(CLIENTS_FILTER, QoS::AtLeastOnce)?;

    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let clients: Clients = Arc::new(Mutex::new(BTreeMap::new()));
    let seen_orders = orders.clone();
    let seen_clients = clients.clone();
    pump(connection, move |topic, payload| {
        if topic.starts_with(STATUS_ROOT) {
            if let Ok(order) = serde_json::from_slice::<Order>(payload) {
                upsert(&seen_orders, order);
            }
        } else if topic.starts_with(CLIENTS_ROOT)
            && let Ok(status) = serde_json::from_slice::<ClientStatus>(payload)
        {
            seen_clients
                .lock()
                .unwrap()
                .insert(status.client, status.status);
        }
    });

    let server = http("0.0.0.0:3002")?;
    println!("dashboard http://localhost:3002");
    serve(server, move |_, path, _| {
        if path == "/api" {
            let list = orders.lock().unwrap();
            let map = clients.lock().unwrap();
            let clients: Vec<_> = map
                .iter()
                .map(|(client, status)| ClientStatus {
                    client: client.clone(),
                    status: status.clone(),
                })
                .collect();
            return Reply::Json(
                serde_json::json!({ "orders": *list, "clients": clients }).to_string(),
            );
        }
        Reply::Html(page(
            "Dashboard",
            r#"<section><h2>Clients</h2><div class="panel" id="clients"></div></section>
<section><h2>Alle Bestellungen</h2><div class="panel" id="orders"></div></section>"#,
            &format!(
                r#"{orders_js}
poll('/api',d=>{{document.getElementById('clients').innerHTML=rows(d.clients,{{
  head:'<thead><tr><th>Client</th><th>Status</th></tr></thead>',
  row:c=>'<tr><td>'+c.client+'</td><td>'+pill(c.status)+'</td></tr>',
  empty:'Keine Clients verbunden.'}})}});"#,
                orders_js = order_rows_js("orders"),
            ),
        ))
    });
    Ok(())
}

// -------------------------------------------------------------------- broker

/// An in-process MQTT broker, so the demo runs without installing one.
fn embedded_broker() -> Result<()> {
    let (_, port) = broker();
    let listen = format!("0.0.0.0:{port}").parse()?;
    let connections = ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: 64 * 1024,
        max_inflight_count: 100,
        auth: None,
        external_auth: None,
        dynamic_filters: true, // clients subscribe to filters the broker has not seen yet
    };
    let mut config = rumqttd::Config {
        id: 0,
        router: RouterConfig {
            max_connections: 100,
            max_outgoing_packet_count: 200,
            max_segment_size: 1024 * 1024,
            max_segment_count: 10,
            ..Default::default()
        },
        ..Default::default()
    };
    config.v4 = Some(HashMap::from([(
        "pizza".to_string(),
        ServerSettings {
            name: "pizza".into(),
            listen,
            tls: None,
            next_connection_delay_ms: 10,
            connections,
        },
    )]));

    println!("broker    mqtt://0.0.0.0:{port}");
    Broker::new(config).start()?;
    Ok(())
}

// ----------------------------------------------------------------------- all

/// Blocks until the broker accepts TCP connections, so the clients do not race it.
fn wait_for_broker() -> Result<()> {
    let (host, port) = broker();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect((host.as_str(), port)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!("broker on {host}:{port} did not come up within 10s")
}

/// Runs broker, kitchen, dashboard and customer in one process.
fn all() -> Result<()> {
    thread::spawn(|| {
        if let Err(error) = embedded_broker() {
            eprintln!("broker: {error}");
        }
    });
    wait_for_broker()?;

    for (name, run) in [
        ("kitchen", kitchen as fn() -> Result<()>),
        ("dashboard", dashboard as fn() -> Result<()>),
    ] {
        thread::spawn(move || {
            if let Err(error) = run() {
                eprintln!("{name}: {error}");
            }
        });
    }

    customer()
}

fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("all") => all(),
        Some("broker") => embedded_broker(),
        Some("customer") => customer(),
        Some("kitchen") => kitchen(),
        Some("dashboard") => dashboard(),
        _ => {
            eprintln!("usage: cargo run -- all|broker|customer|kitchen|dashboard");
            eprintln!("env:   MQTT_BROKER (default localhost), MQTT_PORT (default 1883)");
            Ok(())
        }
    }
}
