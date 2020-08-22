use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut producer = zaichik::Client::connect("127.0.0.1:8889").await?;
    let mut consumer = zaichik::Client::connect("127.0.0.1:8889").await?;

    producer
        .create_topic("hello".to_string(), 0, 0)
        .await?;

    // Запишем в hello 100 сообщений
    for _ in 0..100 {
        producer
            .publish("hello".to_string(), "message".to_string().into_bytes())
            .await?
    }

    consumer.subscribe_on("hello".to_string()).await?;

    // Прочитаем 10 и завершим сеанс
    for _ in 0..100 {
        let result = consumer.read_message().await?;
        consumer.commit().await?;
        println!("Result is {:?}", result);
    }

    Ok(())
}
