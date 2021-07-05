import datetime
import time
import requests
import league_of_legends_dao


def get_url_json(url):
    response = requests.get(url)
    return response.json()


if __name__ == '__main__':
    oracle_connection = league_of_legends_dao.OracleConnection()

    while True:
        print(datetime.datetime.now())
        try:
            versions = get_url_json("https://ddragon.leagueoflegends.com/api/versions.json")
            now_version = versions[0]
            language = "ko_KR"
            champion_json = get_url_json(
                f"https://ddragon.leagueoflegends.com/cdn/{now_version}/data/{language}/champion.json")
            row_count = oracle_connection.delete_list(now_version, language)
            print(f"{row_count}개의 행이 삭제되었습니다.")
            row_count = oracle_connection.insert_list(language,
                                                      [champion_json["data"][champion] for champion in
                                                       champion_json["data"]])
            print(f"{row_count}개의 행이 추가되었습니다.")
        except Exception as e:
            print(e)
        time.sleep(3600)
