use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client1 = zaichik::Client::connect("127.0.0.1:8889").await?;
    let mut client2 = zaichik::Client::connect("127.0.0.1:8889").await?;

    client1
        .create_topic("hello".to_string(), 0, 0)
        .await
        .unwrap();

    // Запишем в hello 100 сообщений
    for _ in 0..100 {
        client1
            .publish("hello".to_string(), "message".to_string().into_bytes())
            .await
            .unwrap()
    }

    client2.subscribe_on("hello".to_string()).await.unwrap();

    // Прочитаем 10 и завершим сеанс
    for _ in 0..100 {
        let result = client2.read_message().await.unwrap();
        client2.commit().await.unwrap();
        println!("Result is {:?}", result);
    }

    // У отправителя в итоге будет 100 сообщений во внутреннем буфере, которые он будет удалять
    // при отправке. А получатель все сообщения прочитал.
    // [Sender] Current lag: 100 || Client is waiting for message: false || Subscribed on: {}
    // [Receiver] Current lag: 0 || Client is waiting for message: true || Subscribed on: {"hello"}

    Ok(())
}
