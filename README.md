# Анализ товаров в интернет-магазине
## Метрики для расчета
+ каждые десять минут рассчитывать
    + топ-10 покупаемых товаров за последний час
    + топ-10 просматриваемых товаров за последний час
## Инструмент
**Apache Flink (Scala on Java API)**
## Особенности расчета
+ требуется считывать данные из *Apache Kafka*
+ нужно отправлять рассчитанные метрики в *Elasticsearch* для дальнейшего анализа
+ на вход поступают данные в формате *json*
## Технические фишки
+ использование Scala для Java API
+ обработка состояния с ключевым доступом (transformers.TopGoodsStats)
+ использование триггера
+ использование собственного десериалайзера (deserializers.EventDeserializer)