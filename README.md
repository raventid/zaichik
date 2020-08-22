# zaichik

В данном проекте мы посмотрим, как можно разработать очень простой брокер сообщений с помощью языка Rust и фреймворка Tokio.
Мы посмотрим на то, как построить свой протокол на основе TCP, как организовать передачу данных с помощью каналов
и как можно организовать хранение данных, к которым нужен доступ из разных потоков. Поехали!

## Общий подход
Существует множество вариантов организации кода для брокера. Основной единицей для
хранения сообщений в зайчике будет топик (topic). Можно предположить, что для топика мы воспользуемся какой-то
специальной структурой данных, например массивом или очередью, для того, чтобы хранить в нем сообщения и потом вычитывать
оттуда с помощью подписки на него, но нет. В зайчике мы воспользуемся другим подходом.

Для каждого клиента мы создадим свою очередь, где будем хранить копии всех сообщений, которые нам интересны.
Это поможет нам избежать некотых сложностей с синхронизацией ресурсов, а так же откроет пространство для
разных интересных ходов.

## Как примерно работает
В main.rs обычный сервер на токио, обработчик сокета запускается в новой таске. В protocol.rs - фреймы для нашего протокола.

Каждая таска стартует вспомогательный SubscriptionManager. Он внутри использует MessageBuffer.
MessageBuffer хранит сообщения и перед отправкой пользователю проверяет их на дубли, ttl и так далее.

Сам SubscriptionManager удерживает Writer для tcp, тогда как в основном обработчике в main.rs находится Reader часть.
Они общаются друг с другом по broadcast каналу.

topic_registry - это реестр топиков, там хранится имя топика и его настройки по ретеншену
и дедупу.

## Резюме
Общий подход, который использовался здесь ориентирован на медленных клиентов, все было сделано для этого.
А именно, клиент отправляет фрейм подтверждения на каждый запрос, у каждого клиента свой лаг и он видит общий лаг по
очереди, а не по топику. В рамках одного подключения можно подписаться на любое количество топиков, они мультиплексированы.
Доступна дедупликация и retention в рамках одного консьюмера. Нет разделения на продьюсер и консьюмер, можно писать и читать свои сообщения.

Реализация с индивидуальными буфферами для каждого потребителя имеет свои преимущества (меньше синхронизировать, проще код писать), но недостатков тоже много.
Задача сделать общий ретеншн в таком варианте решается сложно и все равно требует дополнительную структуру.

Еще один не самый удачный шаг - это общий броадкаст-канал для всех сообщений. Из-за частых коммитов, он оказывается
наполовину заполнен сообщениями, которые не интересны большинству потребителей и это не особо эффективно.

Каждый подписчик хранит все сообщения (да, у каждого подписчика копия всех сообщений).
Это сделано в продолжение идеи о том, что каждый подписчик может делать со своей копией базы, что хочет, а фильтрация
сообщений происходит на последнем шаге, непосредственно перед отправкой. Перестарался в попытке
избежать синхронизаций.

Для того, чтобы посмотреть версию построеную не вокруг одного клиента, а вокруг топика (с retentino и compaction) можно переключиться на ветку feature/smarter-broadcast (https://github.com/raventid/zaichik/pull/1)

## Что можно улучшить
- Общий ретеншн (можно для этого переделать месседж буффер)
- Транзакции вместо коммита одного фрейма.
- Побольше тестов.
- Поменьше копирований и аллокаций.
- Для персональных сообщений клиентам стоит сделать отдельный канал,
может быть mpsc и смержить его с броадкастом в один стрим.
- Сделать отдельные кодеки для Sink, Stream
- Выключить лишние фичи токио
- Чтобы не слишком скрывать основные структуры импорты в проекте часто встречаются полные пути до зависимостей, не везде есть логгирование и иногда нет обработки ошибок (особенно это заметно в клиенте, но он сделан только для экзамплов)
- MessageBuffer сделан на VecDeque. Это не очень удачная попытка повторить sync::broadcast. Почему бы его и не использовать для шины.


## Разработка
Ход разработки можно смотреть в коммитах к репозиторию, а для ознакомления с исходным кодом можно прочитать комментарии и 
README.

commit 81b5776d72d485bdb08ab57235b6796a694bad5c
Date:   Wed Aug 19 04:22:57 2020 +0300

    final version of broker based on broadcast
    
    Extended protocl and tied together SubscriptionManager
    TopicRegistry, Server.

commit 54758472b78af5b6525d7627b7080a956e08c032
Date:   Tue Aug 18 20:26:57 2020 +0300

    redo echo server with broadcast style
    
    In this pull request we are making the first step to wire together
    our subscription manager and tcp-server. In our case we are just
    adding broadcast and passing ownership of write part of a tcp stream
    inside our external manager

commit 76b944ce5dda1d36e7702f7ba053fc4371632de6
Date:   Tue Aug 18 19:42:20 2020 +0300

    add internal subscriber, message buffer
    
    In this commit we are adding a few components which are not
    tied together yet. We are adding subscriber, message buffer,
    topic_registry which are the key components for our future
    system

commit e32a700d5a549cb7883da6fe4eb8ad213e2d30eb
Date:   Tue Aug 18 02:03:09 2020 +0300

    adds simple framed tcp echo server
    
    Also here we are adding examples directory with simple write and read to and
    from our server. Also here we demonstrate how to split tcp stream into stream
    and sink parts, so we can separte incoming and outcoming traffic.

commit 084d07bfc71e161e69ff47d97134234a16095e1d
Date:   Tue Aug 18 00:11:28 2020 +0300

    create custom frame with tokio codec


## Запуск
Чтобы видеть все логи, можно запустить сервер брокера вот так:
```
RUST_LOG=debug cargo run
```

Дальше можно запускать разные примеры. Например эхо-клиент, который шлет сообщение и получает его сам.
```
cargo run --example echo
```

Пример с двумя продьюсерами и потребителями.
```
cargo run --example two_clients
```
