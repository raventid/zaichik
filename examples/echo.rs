use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = std::env::vars()
        .find(|(key, _value)| key == "PORT")
        .map(|(_key, value)| value)
        .unwrap_or_else(|| "8889".to_string());

    let mut client = zaichik::Client::connect(&format!("127.0.0.1:{}", port)).await?;

    client.subscribe_on("hello".to_string()).await?;

    client
        .publish(
            "hello".to_string(),
            Some("key".to_string()),
            "message".to_string().into_bytes(),
        )
        .await?;

    let result = client.read_message().await?;

    println!("Result is {:?}", result);

    client.close().await?;

    Ok(())
}
