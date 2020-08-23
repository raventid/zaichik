# zaichik

В данном проекте мы посмотрим, как можно разработать очень простой брокер сообщений с помощью языка Rust и фреймворка Tokio.
Мы посмотрим на то, как построить свой протокол на основе TCP, как организовать передачу данных с помощью каналов
и как можно организовать хранение данных, к которым нужен доступ из разных потоков. Поехали!

### Для того, чтобы посмотреть первую версию, построенную вокруг одного клиента, а не топика (с retention и compaction на уровне клиента) можно переключиться на ветку master

## Описание
Существует множество вариантов организации кода для брокера. Основной единицей для
хранения сообщений в Зайчике будет топик (topic).

Topic представляет собой tokio::sync::broadcast канал, построенный на основе VecDeque. Клиенты представлены
отдельными асинхронными компонентами.

В итоге наш брокер соответствует следующим критериям:

- Асинхронная обработка команд (CreateTopic, Subscribe, Unsubscribe, Publish, Commit, Close)
- Retention (задается через retention_ttl)
- Compaction (в определенное временное окно, задается с помощью compaction_window)
- Подтверждение получения с помощью Commit
- Автоматическое создание топиков, если сообщение пишется в несуществующий топик
- Топик невозможно удалить после создания
- Publishing и Subscribing в рамках одного tcp подключения и клиента
- Крутое название

## Как примерно работает
В main.rs обычный tcp-сервер на tokio, обработчик сокета запускается в новой таске.

protocol.rs - фреймы для нашего протокола. Энкодеры и декодеры.

Каждая таска стартует вспомогательный SubscriptionManager. Он, с помощью tokio::select! подписывается на два стрима.
Первый стрим - это комманды от клиента - Subscribe, Publish (tokio::mpsc). Второй - это мультиплексированная подписка на все топики (tokio::broadcast).

Сам SubscriptionManager удерживает Writer для tcp, тогда как в основном обработчике в main.rs находится Reader часть.

TopicRegistry - реестр ссылок на TopicController. Использует глобальную блокировку при создании нового топика.

TopicController - компонент, отвечающий за один топик. В рамках одного топика, используется глобальный лок на publish,
для того, чтобы безопасно выполнить compaction в рамках топика. Subscribe и другие операции происходят без блокировок.
Основным элементом TopicController является tokio::syn::broadcast, который является хранилищем сообщений.
Для retained сообщений используется вспомогательный Vec, а для compaction HashMap.

lib.rs, examples - клиент и примеры использования в разных конфигурациях

## Что можно улучшить
- Улучшить обработку и логгирование ошибок
- Сделать retry для отправки сообщений
- Транзакции вместо коммита одного фрейма или batch опцию
- Сделать отдельные кодеки для Sink, Stream

## Разработка
Ход разработки можно смотреть в коммитах к репозиторию, а для ознакомления с исходным кодом можно прочитать комментарии и 
README.

##### Текущая ветка еще не попала в мастер, поэтому здесь приведен список коммитов из первой версии, последние изменения лучше смотреть в git log

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
Чтобы видеть все логи, можно запустить сервер брокера вот так (порт можно не писать):
```
 RUST_LOG=debug PORT=8889 cargo run
```

##### При запуске примеров лучше перезапускать сервер брокера, иначе, в зависимости от настроек retention или compaction (при совпадении имен топиков в примерах) он может вести себя неожиданно.
##### Например, если запустить compaction пример два раза, то во второй раз не дойдет ни одного сообщения, потому что они закомпактятся.

Дальше можно запускать разные примеры. Например эхо-клиент, который шлет сообщение и получает его сам.
```
PORT=8889 cargo run --example echo
```

Пример с сохранением сообщения в топике.
```
PORT=8889 cargo run --example retention
```

Пример с удалением дубликатов по ключу.
```
PORT=8889 cargo run --example compaction  
```
