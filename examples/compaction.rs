use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = std::env::vars()
        .find(|(key, _value)| key == "PORT")
        .map(|(_key, value)| value)
        .unwrap_or_else(|| "8889".to_string());

    let mut producer = zaichik::Client::connect(&format!("127.0.0.1:{}", port)).await?;
    let mut consumer = zaichik::Client::connect(&format!("127.0.0.1:{}", port)).await?;

    producer
        .create_topic("hello".to_string(), 0, 10_000)
        .await?;

    // Запишем в hello 100 сообщений
    for _ in 0..100 {
        producer
            .publish(
                "hello".to_string(),
                Some("key1".to_string()),
                "message".to_string().into_bytes(),
            )
            .await?
    }

    producer
        .publish(
            "hello".to_string(),
            Some("key2".to_string()),
            "message1".to_string().into_bytes(),
        )
        .await?;

    consumer.subscribe_on("hello".to_string()).await?;

    let message = consumer.read_message().await?;
    consumer.commit().await?;
    println!("Result is {:?}", message);

    let message1 = consumer.read_message().await?;
    consumer.commit().await?;
    println!("Result is {:?}", message1);

    // В выводе на экран можно увидеть, чо мы пропустили дублированные сообщения
    // Result is Publish { topic: "hello", key: Some("key1"), payload: [109, 101, 115, 115, 97, 103, 101] }
    // Result is Publish { topic: "hello", key: Some("key2"), payload: [109, 101, 115, 115, 97, 103, 101, 49] }

    Ok(())
}
