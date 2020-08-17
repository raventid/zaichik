use bytes;
use serde::{Deserialize, Serialize};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

// Фрейм нашего протокола. Несмотря на то, что мы используем
// TCP, где данные передаются просто, как стрим байтов мы
// можем выделить логические блоки, которые называются фреймами.
// Используя трейты Serialize и Deserialize из tokio_util мы можем
// реализовать парсер для такого фрейма, который будет создавать его из потока
// байтов.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum ZaichikFrame {
    Publish { topic: String, payload: Vec<u8> },
    Subscribe { topic: String },
}

// Кодек позволяет нам превратить наш фрейм в байты и обратно.
// Мы для передачи данных будем использовать бинкод.
pub struct ZaichikCodec;

impl ZaichikCodec {
    pub fn new() -> ZaichikCodec {
        ZaichikCodec {}
    }
}

impl Encoder for ZaichikCodec {
    type Item = ZaichikFrame;
    type Error = io::Error;

    fn encode(
        &mut self,
        item: ZaichikFrame,
        buffer: &mut bytes::BytesMut,
    ) -> Result<(), io::Error> {
        let encoded: Vec<u8> = bincode::serialize(&item).unwrap();
        buffer.extend(encoded);
        Ok(())
    }
}

impl Decoder for ZaichikCodec {
    type Item = ZaichikFrame;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<ZaichikFrame>, io::Error> {
        if !buf.is_empty() {
            match bincode::deserialize::<ZaichikFrame>(&buf[..]) {
                Ok(decoded) => match bincode::serialized_size(&decoded) {
                    Ok(already_consumed) => {
                        let _consumed_frame = buf.split_to(already_consumed as usize);
                        Ok(Some(decoded))
                    }
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to calculate serialized size",
                    )),
                },
                Err(_err) => {
                    buf.clear();
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to decode Frame, cleaning buffer",
                    ))
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_encoder_decoder() {
        let frame = ZaichikFrame::Publish {
            topic: String::from("topic"),
            payload: vec![1, 2, 3, 4, 5],
        };

        let mut buffer = bytes::BytesMut::new();
        ZaichikCodec::new()
            .encode(frame.clone(), &mut buffer)
            .unwrap();

        let decoded = ZaichikCodec::new().decode(&mut buffer).unwrap().unwrap();
        assert_eq!(frame, decoded)
    }

    #[test]
    fn test_frame_encoder_decoder_on_multiplexed_stream() {
        let frame1 = ZaichikFrame::Publish {
            topic: String::from("topic1"),
            payload: vec![1, 2, 3, 4, 5],
        };

        let frame2 = ZaichikFrame::Publish {
            topic: String::from("topic2"),
            payload: vec![1, 2, 3, 4, 5],
        };

        let mut buffer = bytes::BytesMut::new();

        ZaichikCodec::new()
            .encode(frame1.clone(), &mut buffer)
            .unwrap();
        ZaichikCodec::new()
            .encode(frame2.clone(), &mut buffer)
            .unwrap();

        let decoded1 = ZaichikCodec::new().decode(&mut buffer).unwrap().unwrap();
        let decoded2 = ZaichikCodec::new().decode(&mut buffer).unwrap().unwrap();

        assert_eq!(frame1, decoded1);
        assert_eq!(frame2, decoded2);
    }
}
