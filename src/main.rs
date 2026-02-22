use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use breakout1_kv_store::Engine;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct SetRequest {
    key: String,
    value: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db = web::Data::new(Engine::load("data.db")?);

    HttpServer::new(move || {
        App::new()
            .app_data(db.clone())
            .route("/", web::get().to(home))
            .route("/set", web::post().to(set_handler))
            .route("/get/{key}", web::get().to(get_handler))
            .route("/del/{key}", web::delete().to(del_handler))
    })
    .bind(" ")?
    .run()
    .await
}

async fn home(_req: HttpRequest) -> impl Responder {
    format!("Welcome!")
}

async fn set_handler(req: web::Json<SetRequest>, engine: web::Data<Engine>) -> impl Responder {
    let op = engine.set(req.key.clone().into_bytes(), req.value.clone().into_bytes());
    match op {
        Ok(_) => HttpResponse::Ok().body("OK"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

async fn get_handler(req: web::Path<String>, engine: web::Data<Engine>) -> impl Responder {
    let op = engine.get(req.as_bytes());
    match op {
        Ok(Some(val)) => HttpResponse::Ok().body(val),
        Ok(None) => HttpResponse::NotFound().body("Key is not found"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string())
    }
}

async fn del_handler(req: web::Path<String>, engine: web::Data<Engine>) -> impl Responder {
    let op = engine.del(req.clone().into_bytes());
    match op {
        Ok(_) => HttpResponse::Ok().body("OK"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
