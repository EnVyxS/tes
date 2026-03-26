// File: src/main.rs (Bagian 1)
// Arsitektur: DIVA V30 - The Microstructure Hybrid
// Modul: Fase 1 & 2 (Dual-Stream Ingestor: Kraken L3 + Bybit L2 -> QuestDB)

use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::env;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

/// ==============================================================================
/// STRUKTUR SKEMA BERLANGGANAN (SUBSCRIPTION PAYLOADS)
/// ==============================================================================
#[derive(Serialize)]
struct KrakenSubscriptionParams {
    channel: String,
    symbol: Vec<String>,
    depth: u32,
    snapshot: bool,
    token: String,
}

#[derive(Serialize)]
struct KrakenSubscription {
    method: String,
    params: KrakenSubscriptionParams,
}

#[derive(Serialize)]
struct BybitSubscription {
    op: String,
    args: Vec<String>,
}

/// Status Pesanan RAM L3
struct OrderSnapshot {
    price: f64,
    volume: f64,
}

/// ==============================================================================
/// FUNGSI UTAMA (THE DUAL-STREAM INGESTOR RUNTIME)
/// ==============================================================================
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[DIVA-ARCHITECT] Mengaktifkan The Ingestor Lapisan 1 (Dual-Stream Mode)...");

    // 1. Ekstraksi Token Autentikasi Keamanan Kraken
    let kraken_ws_token = match env::var("KRAKEN_WS_TOKEN") {
        Ok(token) => token,
        Err(_) => {
            eprintln!("[FATAL] Variabel lingkungan KRAKEN_WS_TOKEN tidak terdeteksi.");
            std::process::exit(1);
        }
    };

    // 2. Terowongan Memori Bebas-Kunci (MPSC) untuk Mencegah Tokio Starvation
    // Saluran transmisi dari Utas Jaringan ke Utas Pekerja (QuestDB & RAM LOB)
    let (tx_kraken, mut rx_kraken) = mpsc::unbounded_channel::<String>();
    let (tx_bybit, mut rx_bybit) = mpsc::unbounded_channel::<String>();

    // ==============================================================================
    // UTAS JARINGAN 1: KRAKEN L3 MBO (ISOLASI ASINKRON)
    // ==============================================================================
    let tx_k = tx_kraken.clone();
    let token_k = kraken_ws_token.clone();
    let kraken_task = tokio::spawn(async move {
        let url = "wss://ws-l3.kraken.com/v2";
        let (ws_stream, response) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("[KRAKEN-L3] FATAL: Kegagalan jabat tangan: {}", e);
                return;
            }
        };
        println!("[KRAKEN-L3] Soket terenkripsi aktif. Status: {}", response.status());

        let (mut write, mut read) = ws_stream.split();

        let sub_payload = KrakenSubscription {
            method: "subscribe".to_string(),
            params: KrakenSubscriptionParams {
                channel: "level3".to_string(),
                symbol: vec!["BTC/USD".to_string()],
                depth: 10,
                snapshot: true,
                token: token_k,
            },
        };

        let sub_msg = serde_json::to_string(&sub_payload).unwrap_or_default();
        if let Err(e) = write.send(Message::Text(sub_msg.into())).await {
            eprintln!("[KRAKEN-L3] Gagal menyuntikkan paket langganan: {}", e);
            return;
        }

        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if tx_k.send(text.to_string()).is_err() {
                        break; // Utas pekerja mati, hentikan pendengaran
                    }
                }
                Ok(Message::Ping(ping)) => {
                    let _ = write.send(Message::Pong(ping)).await;
                }
                Err(e) => {
                    eprintln!("[KRAKEN-L3] Koneksi terputus (Latensi ISP/Anomali): {}", e);
                    break;
                }
                _ => {}
            }
        }
        println!("[KRAKEN-L3] Utas Jaringan terhenti.");
    });

    // ==============================================================================
    // UTAS JARINGAN 2: BYBIT L2 DEPTH (ISOLASI ASINKRON)
    // ==============================================================================
    let tx_b = tx_bybit.clone();
    let bybit_task = tokio::spawn(async move {
        let url = "wss://stream.bybit.com/v5/public/spot";
        let (ws_stream, response) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("[BYBIT-L2] FATAL: Kegagalan jabat tangan: {}", e);
                return;
            }
        };
        println!("[BYBIT-L2] Soket terenkripsi aktif. Status: {}", response.status());

        let (mut write, mut read) = ws_stream.split();

        let sub_payload = BybitSubscription {
            op: "subscribe".to_string(),
            args: vec!["orderbook.50.BTCUSDT".to_string()],
        };

        let sub_msg = serde_json::to_string(&sub_payload).unwrap_or_default();
        if let Err(e) = write.send(Message::Text(sub_msg.into())).await {
            eprintln!("[BYBIT-L2] Gagal menyuntikkan paket langganan: {}", e);
            return;
        }

        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if tx_b.send(text.to_string()).is_err() {
                        break;
                    }
                }
                Ok(Message::Ping(ping)) => {
                    let _ = write.send(Message::Pong(ping)).await;
                }
                Err(e) => {
                    eprintln!("[BYBIT-L2] Koneksi terputus (Latensi ISP/Anomali): {}", e);
                    break;
                }
                _ => {}
            }
        }
        println!("[BYBIT-L2] Utas Jaringan terhenti.");
    });

    // File: src/main.rs (Bagian 2)
    // Arsitektur: DIVA V30 - The Microstructure Hybrid
    // Modul: Fase 1 & 2 (Dual-Stream Ingestor: Kraken L3 + Bybit L2 -> QuestDB)

    // ==============================================================================
    // UTAS PEKERJA (THE VAULT WORKER & RAM LOB MANAGER)
    // ==============================================================================
    let worker_task = tokio::spawn(async move {
        println!("[WORKER] Mesin BTreeMap LOB & Konektor QuestDB menyala.");
        
        let mut qdb_stream = match TcpStream::connect("127.0.0.1:9009").await {
            Ok(stream) => {
                println!("[THE VAULT] Jembatan TCP ILP ke QuestDB terhubung.");
                Some(stream)
            },
            Err(e) => {
                eprintln!("[THE VAULT] Peringatan: Gagal terhubung ke QuestDB: {}", e);
                None
            }
        };

        // Arsitektur RAM: BTreeMap & HashMap untuk Kraken L3
        let mut order_map: HashMap<String, OrderSnapshot> = HashMap::with_capacity(50_000);
        let mut bids_book: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();
        let mut asks_book: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();

        let mut ilp_buffer = String::with_capacity(32768); 
        let mut batch_count = 0;
        let mut flush_interval = interval(Duration::from_millis(1000)); 

        let mut kraken_active = true;
        let mut bybit_active = true;

        loop {
            tokio::select! {
                // SIKLUS A: Pembilasan Data ke HDD Mekanik (Setiap 1 Detik)
                _ = flush_interval.tick() => {
                    if batch_count > 0 {
                        if let Some(stream) = qdb_stream.as_mut() {
                            if let Err(e) = stream.write_all(ilp_buffer.as_bytes()).await {
                                eprintln!("[THE VAULT] Gagal menyiram data ke Disk: {}", e);
                            } else {
                                ilp_buffer.clear();
                                batch_count = 0;
                            }
                        } else {
                            ilp_buffer.clear();
                            batch_count = 0;
                        }
                    }
                }
                
                // SIKLUS B: Ekstraksi Memori L3 dari Saluran Kraken
                msg_k = rx_kraken.recv(), if kraken_active => {
                    match msg_k {
                        Some(raw_text) => {
                            // POISON PILL RECOVERY: Tidak ada penggunaan .unwrap() buta
                            let parsed_json: Value = match serde_json::from_str(&raw_text) {
                                Ok(v) => v,
                                Err(_) => continue, 
                            };

                            if parsed_json.get("channel").and_then(|c| c.as_str()) != Some("level3") {
                                continue; 
                            }

                            if let Some(data_array) = parsed_json.get("data").and_then(|d| d.as_array()) {
                                for item in data_array {
                                    let symbol = item.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
                                    let raw_type = parsed_json.get("type").and_then(|t| t.as_str()).unwrap_or("add");
                                    
                                    let action_val = match raw_type {
                                        "snapshot" | "add" => "add",
                                        "modify" => "modify",
                                        "cancel" => "cancel",
                                        "fill" => "fill",
                                        _ => "add",
                                    };
                                    
                                    for side_key in ["bids", "asks"] {
                                        let side_val = if side_key == "bids" { "buy" } else { "sell" };
                                        
                                        if let Some(side_arr) = item.get(side_key).and_then(|s| s.as_array()) {
                                            for order in side_arr {
                                                let order_id = order.get("order_id").and_then(|o| o.as_str()).unwrap_or("N/A");
                                                
                                                let mut current_price = order.get("limit_price")
                                                    .and_then(|p| p.as_f64().or_else(|| p.as_str().and_then(|s| s.parse::<f64>().ok())))
                                                    .unwrap_or(0.0);

                                                let current_volume = order.get("order_qty")
                                                    .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok())))
                                                    .unwrap_or(0.0);

                                                // MANIPULASI B-TREE MAP & HASHMAP (LOB RAM MURNI)
                                                if action_val == "add" || action_val == "snapshot" {
                                                    order_map.insert(order_id.to_string(), OrderSnapshot { 
                                                        price: current_price, 
                                                        volume: current_volume 
                                                    });
                                                    let book = if side_val == "buy" { &mut bids_book } else { &mut asks_book };
                                                    *book.entry(OrderedFloat(current_price)).or_insert(0.0) += current_volume;
                                                    
                                                } else if action_val == "cancel" || action_val == "fill" {
                                                    if let Some(existing_order) = order_map.remove(order_id) {
                                                        current_price = existing_order.price; // Re-sync harga dari RAM
                                                        let book = if side_val == "buy" { &mut bids_book } else { &mut asks_book };
                                                        let key = OrderedFloat(current_price);
                                                        if let Some(vol) = book.get_mut(&key) {
                                                            *vol -= existing_order.volume;
                                                            if *vol <= 0.000001 { // Toleransi presisi Float64
                                                                book.remove(&key);
                                                            }
                                                        }
                                                    }
                                                    
                                                } else if action_val == "modify" {
                                                    if let Some(existing_order) = order_map.get_mut(order_id) {
                                                        current_price = existing_order.price; 
                                                        let volume_diff = current_volume - existing_order.volume;
                                                        existing_order.volume = current_volume;
                                                        
                                                        let book = if side_val == "buy" { &mut bids_book } else { &mut asks_book };
                                                        let key = OrderedFloat(current_price);
                                                        if let Some(vol) = book.get_mut(&key) {
                                                            *vol += volume_diff;
                                                            if *vol <= 0.000001 {
                                                                book.remove(&key);
                                                            }
                                                        }
                                                    }
                                                }

                                                // HUKUM THE TIME DRIFT: Ekstraksi Exchange Timestamp Absolut (Mikrodetik -> Nanodetik ILP)
                                                let timestamp_str = order.get("timestamp").and_then(|t| t.as_str()).unwrap_or("");
                                                let epoch_micros = match DateTime::parse_from_rfc3339(timestamp_str) {
                                                    Ok(dt) => dt.timestamp_micros(),
                                                    Err(_) => 0, 
                                                };

                                                if epoch_micros > 0 {
                                                    let ilp_line = format!(
                                                        "kraken_mbo,symbol={},action={},side={} order_id=\"{}\",price={},volume={} {}\n",
                                                        symbol, action_val, side_val, order_id, current_price, current_volume, epoch_micros * 1000
                                                    );
                                                    
                                                    ilp_buffer.push_str(&ilp_line);
                                                    batch_count += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        None => kraken_active = false,
                    }
                }

                // SIKLUS C: Ekstraksi Memori L2 dari Saluran Bybit
                msg_b = rx_bybit.recv(), if bybit_active => {
                    match msg_b {
                        Some(raw_text) => {
                            let parsed_json: Value = match serde_json::from_str(&raw_text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            let data_obj = match parsed_json.get("data") {
                                Some(d) => d,
                                None => continue,
                            };

                            let symbol = data_obj.get("s").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
                            
                            // Bybit v5 mengirimkan Exchange Timestamp dalam bentuk milidetik pada field "ts"
                            let epoch_ms = parsed_json.get("ts").and_then(|t| t.as_i64()).unwrap_or(0);
                            
                            if epoch_ms > 0 {
                                // Ekstraktor In-line untuk membongkar Array Bybit menjadi String yang dipisahkan koma
                                // FIX TERAPAN: Menghapus deklarasi 'mut' pada closure ini.
                                let extract_arrays = |key: &str| -> (String, String) {
                                    let mut prices = Vec::new();
                                    let mut vols = Vec::new();
                                    if let Some(arr) = data_obj.get(key).and_then(|a| a.as_array()) {
                                        for item in arr {
                                            if let Some(pair) = item.as_array() {
                                                if pair.len() >= 2 {
                                                    let p = pair[0].as_str().unwrap_or("0");
                                                    let v = pair[1].as_str().unwrap_or("0");
                                                    prices.push(p.to_string());
                                                    vols.push(v.to_string());
                                                }
                                            }
                                        }
                                    }
                                    (prices.join(","), vols.join(","))
                                };

                                let (bids_p, bids_v) = extract_arrays("b");
                                let (asks_p, asks_v) = extract_arrays("a");

                                // Cegah injeksi ILP kosong yang memicu anomali Parse Exception di QuestDB
                                if !bids_p.is_empty() || !asks_p.is_empty() {
                                    let epoch_nanos = epoch_ms * 1_000_000;
                                    let ilp_line = format!(
                                        "bybit_l2,symbol={} bids_price=\"{}\",bids_vol=\"{}\",asks_price=\"{}\",asks_vol=\"{}\" {}\n",
                                        symbol, bids_p, bids_v, asks_p, asks_v, epoch_nanos
                                    );
                                    ilp_buffer.push_str(&ilp_line);
                                    batch_count += 1;
                                }
                            }
                        },
                        None => bybit_active = false,
                    }
                }
                
                // Pintu Keluar Jika Seluruh Saluran Jaringan Mati
                _ = async {
                    if !kraken_active && !bybit_active {
                        std::future::ready(()).await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    break;
                }
            }
        }
        
        if batch_count > 0 {
            if let Some(stream) = qdb_stream.as_mut() {
                let _ = stream.write_all(ilp_buffer.as_bytes()).await;
                println!("[THE VAULT] {} sisa baris memori (L2 & L3) disiram secara paksa.", batch_count);
            }
        }
        println!("[WORKER] Saluran tertutup. Utas Pekerja mati secara elegan.");
    });

    // ==============================================================================
    // PROTOKOL PENUTUPAN ELEGAN (GRACEFUL SHUTDOWN)
    // ==============================================================================
    println!("[SYSTEM] Sirkuit penerimaan stabil. Tekan [Ctrl+C] untuk mematikan secara elegan.");
    
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("\n[SYSTEM] Sinyal Interupsi (Ctrl+C) terdeteksi! Menginisiasi Protokol Graceful Shutdown...");
        },
        Err(err) => {
            eprintln!("[SYSTEM] Gagal mendengarkan sinyal Ctrl+C: {}", err);
        },
    }

    // Membunuh transmisi, memberi tahu Utas Pekerja bahwa tidak ada sisa antrean JSON baru
    drop(tx_kraken);
    drop(tx_bybit);

    println!("[SYSTEM] Memutus suplai data baru. Menunggu Utas Pekerja menguras antrean memori ke QuestDB...");

    // Menunggu Utas Pekerja selesai mem-parsing sisa antrean dan menyiram memori ke Disk HDD
    let _ = worker_task.await;

    // Utas WebSocket akan otomatis gugur dengan bersih karena salurannya telah diputus
    let _ = kraken_task.await;
    let _ = bybit_task.await;

    println!("[DIVA-ARCHITECT] Mesin mati total. Keamanan RAM dan HDD terjaga. Sampai jumpa.");
    Ok(())
}