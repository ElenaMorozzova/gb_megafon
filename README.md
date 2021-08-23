# Курсовой проект к видеокурсу от Megafon
* Для получения предсказаний тестового датасета нужно запустить ноутбук code_to_run, предварительно запустив luigid
* Для пайплайна требуется весь набор данных в т.ч. датасет features.csv
* Рабочий ноутбук workings содержит сравнение нескольких моделей, итоговая модель - xgboost
* Также необходимы в папке feature_selector.py, utils.py, column_selector.py

## Задача:
* Построить алгоритм, который для каждой пары пользователь-услуга определит вероятность подключения услуги.

## Данные:
* data_train.csv: id, vas_id, buy_time, target: информация об отклике абонентов на предложение подключения одной из услуг. Каждому пользователю может быть сделано несколько предложений в разное время, каждое из которых он может или принять, или отклонить.
* features.csv.zip: id, <feature_list> : нормализованный анонимизированный набор признаков, характеризующий профиль потребления абонента. Эти данные привязаны к определенному времени, поскольку профиль абонента может меняться с течением времени.
* data_test.csv: id, vas_id, buy_time: тестовый набор данных
Данные train и test разбиты по периодам – на train доступно 4 месяцев, а на test отложен последующий месяц.
## Описание данных:
* target - целевая переменная, где 1 означает подключение услуги, 0 - абонент не подключил услугу соответственно.
* buy_time - время покупки, представлено в формате timestamp 
* id - идентификатор абонента 
* vas_id - подключаемая услуга 

## Метрика:
Осуществляется скором f1, невзвешенным образом, как например делает функция sklearn.metrics.f1_score(…, average=’macro’).
