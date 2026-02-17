from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os


def archive_inactive_users():
    # Подключение к MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]

    # Основная и архивная коллекции
    source_collection = db["user_events"]
    archive_collection = db["archived_users"]

    # Текущая дата для отчёта
    current_date = datetime.now()
    report_date = current_date.strftime("%Y-%m-%d")

    # Пороговые даты
    thirty_days_ago = current_date - timedelta(days=30)
    fourteen_days_ago = current_date - timedelta(days=14)

    print(f" Текущая дата: {report_date}")
    print(f" Ищем пользователей, зарегистрированных до: {thirty_days_ago.strftime('%Y-%m-%d')}")
    print(f" Последняя активность до: {fourteen_days_ago.strftime('%Y-%m-%d')}")

    # Находим пользователей для архивации
    pipeline = [
        # Группируем по user_id
        {
            "$group": {
                "_id": "$user_id",
                "last_activity": {"$max": "$event_time"},
                "registration_date": {"$first": "$user_info.registration_date"},
                "email": {"$first": "$user_info.email"},
                "events": {"$push": "$$ROOT"}  # Сохраняем все события
            }
        },
        # Регистрация > 30 дней назад и последняя активность < 14 дней назад
        {
            "$match": {
                "registration_date": {"$lt": thirty_days_ago},
                "last_activity": {"$lt": fourteen_days_ago}
            }
        }
    ]

    # Агрегация
    inactive_users = list(source_collection.aggregate(pipeline))

    archived_user_ids = []

    if inactive_users:
        print(f" Найдено {len(inactive_users)} неактивных пользователей")

        # Перемещаем каждого пользователя в архив
        for user_data in inactive_users:
            user_id = user_data["_id"]

            # Создаем архивный документ
            archive_doc = {
                "user_id": user_id,
                "email": user_data["email"],
                "registration_date": user_data["registration_date"],
                "last_activity": user_data["last_activity"],
                "archived_date": current_date,
                "total_events": len(user_data["events"]),
                "events": user_data["events"]  # Сохраняем все события
            }

            # Вставляем в архивную коллекцию
            archive_collection.insert_one(archive_doc)

            # Удаляем все события пользователя из основной коллекции
            source_collection.delete_many({"user_id": user_id})

            archived_user_ids.append(user_id)
            print(f" Пользователь {user_id} архивирован")

        # Сортируем ID
        archived_user_ids.sort()
    else:
        print(" Неактивных пользователей не найдено")

    # Создаём отчёт
    report = {
        "date": report_date,
        "archived_users_count": len(archived_user_ids),
        "archived_user_ids": archived_user_ids
    }

    # Формируем имя файла отчёта
    report_filename = f"{report_date}.json"

    # Сохраняем отчёт в JSON
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n Отчёт сохранён в файл: {report_filename}")

    client.close()
    return report

if __name__ == "__main__":
    try:
        # Запускаем архивацию
        report = archive_inactive_users()
        print(" Архивация прошла успешно")
    except Exception as e:
        print(f"\n Ошибка при выполнении: {e}")
        exit(1)