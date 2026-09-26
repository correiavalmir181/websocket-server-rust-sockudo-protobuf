// ============================================================================
// KEEPALIVE HTTP MODULE - Mantém servidor ativo em plataformas como Render
// ============================================================================
// Versão SIMPLIFICADA - apenas pings essenciais para evitar sleep
// ============================================================================

use tokio::time::interval;
use std::time::Duration;
use std::sync::Arc;
use chrono::Local;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: String,
}

pub struct KeepAliveManager {
    base_url: String,
    is_production: bool,
    client: reqwest::Client,
}

impl KeepAliveManager {
    pub fn new(port: u16) -> Self {
        // Detecta se está no Render (produção) ou local (dev)
        let is_production = std::env::var("RENDER").is_ok();
        
        let base_url = if is_production {
            // Pega hostname do Render ou usa service name
            let hostname = std::env::var("RENDER_EXTERNAL_HOSTNAME")
                .unwrap_or_else(|_| format!("{}.onrender.com", 
                    std::env::var("RENDER_SERVICE_NAME").unwrap_or_default()));
            format!("https://{}", hostname)
        } else {
            format!("http://localhost:{}", port)
        };
        
        // Cliente HTTP com headers corretos para evitar erros de WebSocket
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("KeepAlive-Bot/1.0")  // User-Agent para identificação
            .build()
            .expect("Falha ao criar HTTP client");
        
        Self {
            base_url,
            is_production,
            client,
        }
    }
    
    // Função simples: faz ping com headers HTTP corretos
    async fn ping(&self) {
        let url = format!("{}/health", self.base_url);
        
        // Extrair hostname da URL para header Host
        let host = self.base_url
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        
        match self.client
            .get(&url)
            .header("Host", host)
            .header("Accept", "application/json")
            .header("Origin", &self.base_url)  // Origin para CORS
            .header("Referer", &self.base_url)  // Referer para parecer navegador
            .header("Connection", "keep-alive")  // NÃO "upgrade" (evita WebSocket)
            .header("Cache-Control", "no-cache")
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    println!("🏓 Keepalive OK ({})", response.status());
                } else {
                    println!("⚠️ Keepalive status: {}", response.status());
                }
            }
            Err(e) => {
                // Log erro para debug
                eprintln!("❌ Keepalive erro: {}", e);
            }
        }
    }
    
    // Inicia loop de keepalive (apenas em produção)
    pub async fn start(self: Arc<Self>) {
        if !self.is_production {
            println!("🏓 KeepAlive desabilitado (desenvolvimento)");
            return;
        }
        
        println!("🏓 KeepAlive ativado - ping a cada 8 minutos");
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(8 * 60)); // 8 minutos
            interval.tick().await; // Pula primeiro tick
            
            loop {
                interval.tick().await;
                self.ping().await;
            }
        });
    }
}

// Endpoint handler simples para /health
pub fn get_health_response() -> HealthResponse {
    HealthResponse {
        status: "ok".to_string(),
        timestamp: Local::now().to_rfc3339(),
    }
}
