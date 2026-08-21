use iroh::{Endpoint, endpoint::presets::{self}};

#[tokio::main]
async fn main() {
    let Ok(endpoint) = Endpoint::bind(presets::N0).await else { 
        println!("Couldn't create endpoint");
        return 
    };

    endpoint.online().await;
}

pub async fn rtp_audio_sender() {
    loop {
        
    }
}

pub async fn rtp_frame_sender() {
    loop {

    }
}