import requests
import pandas as pd
from sqlalchemy import create_engine
import json

DATABASE_URL = 'mysql+pymysql://username:password@hostname/schema_name'
WEBHOOK_URL = 'API URL'

def fetch_all_tasks():
    start = 0
    all_tasks = []
    
    while True:
        payload = {
            'order': {'ID': 'asc'},
            'filter': {},
            'start': start
        }

        try:
            response = requests.post(WEBHOOK_URL, json=payload)
            response.raise_for_status()
            result = response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break
        except ValueError:
            print("Invalid JSON response")
            break

        tasks = result.get('result')
        if isinstance(tasks, list):
            all_tasks.extend(tasks)

            if 'next' in result:
                start = result['next']
            else:
                break
        else:
            print("No tasks found or unexpected response format.")
            break

    return all_tasks

def main():
    tasks = fetch_all_tasks()
    df = pd.DataFrame(tasks)
    
    if df.empty:
        print("No tasks to save.")
        return

    # Convert dict columns to JSON strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    engine = create_engine(DATABASE_URL)

    try:
        df.to_sql('tasks', con=engine, if_exists='replace', index=False)
        print("Data saved to MySQL database.")
    except Exception as e:
        print(f"Error saving data: {e}")

if __name__ == "__main__":
    main()


