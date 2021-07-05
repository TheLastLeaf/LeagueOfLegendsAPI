import json
import sqlalchemy
import cx_Oracle


class OracleConnection:
    def __init__(self):
        host = "ririnto.asuscomm.com"
        port = "1521"
        dns = "XE"
        user = "BROWNIE"
        password = "java"
        dns = cx_Oracle.makedsn(host, port, sid=dns)

        c_str = f'oracle://{user}:{password}@{dns}'

        self.__engine = sqlalchemy.create_engine(c_str)

        with open("league_of_legends_sql.json", "r") as league_of_legends_sql_json:
            self.league_of_legends_sql_json = json.load(league_of_legends_sql_json)

    def insert(self, language, champion):
        return self.__engine.connect().execute(sqlalchemy.text(self.league_of_legends_sql_json["insert"]),
                                               VERSION=champion["version"],
                                               LANGUAGE=language,
                                               ID=champion["id"],
                                               KEY=champion["key"],
                                               NAME=champion["name"],
                                               TITLE=champion["title"],
                                               BLURB=champion["blurb"],
                                               INFO_ATTACK=champion["info"]["attack"],
                                               INFO_DEFENSE=champion["info"]["defense"],
                                               INFO_MAGIC=champion["info"]["magic"],
                                               INFO_DIFFICULTY=champion["info"]["difficulty"],
                                               IMAGE_FULL=champion["image"]["full"],
                                               IMAGE_SPRITE=champion["image"]["sprite"],
                                               IMAGE_GROUP=champion["image"]["group"],
                                               IMAGE_X=champion["image"]["x"],
                                               IMAGE_Y=champion["image"]["y"],
                                               IMAGE_W=champion["image"]["w"],
                                               IMAGE_H=champion["image"]["h"],
                                               TAGS=str(champion["tags"]),
                                               PARTYPE=champion["partype"],
                                               STATS_HP=champion["stats"]["hp"],
                                               STATS_HPPERLEVEL=champion["stats"]["hpperlevel"],
                                               STATS_MP=champion["stats"]["mp"],
                                               STATS_MPPERLEVEL=champion["stats"]["mpperlevel"],
                                               STATS_MOVESPEED=champion["stats"]["movespeed"],
                                               STATS_ARMOR=champion["stats"]["armor"],
                                               STATS_ARMORPERLEVEL=champion["stats"]["armorperlevel"],
                                               STATS_SPELLBLOCK=champion["stats"]["spellblock"],
                                               STATS_SPELLBLOCKPERLEVEL=champion["stats"]["spellblockperlevel"],
                                               STATS_ATTACKRANGE=champion["stats"]["attackrange"],
                                               STATS_HPREGEN=champion["stats"]["hpregen"],
                                               STATS_HPREGENPERLEVEL=champion["stats"]["hpregenperlevel"],
                                               STATS_MPREGEN=champion["stats"]["mpregen"],
                                               STATS_MPREGENPERLEVEL=champion["stats"]["mpregenperlevel"],
                                               STATS_CRIT=champion["stats"]["crit"],
                                               STATS_CRITPERLEVEL=champion["stats"]["critperlevel"],
                                               STATS_ATTACKDAMAGE=champion["stats"]["attackdamage"],
                                               STATS_ATTACKDAMAGEPERLEVEL=champion["stats"]["attackdamageperlevel"],
                                               STATS_ATTACKSPEEDPERLEVEL=champion["stats"]["attackspeedperlevel"],
                                               STATS_ATTACKSPEED=champion["stats"]["attackspeed"]).rowcount

    def insert_list(self, language, champions):
        return sum(self.insert(language, champion) for champion in champions)

    def delete_list(self, version, language):
        return self.__engine.connect().execute(sqlalchemy.text(self.league_of_legends_sql_json["delete_list"]),
                                               VERSION=version,
                                               LANGUAGE=language).rowcount

    def delete_all(self):
        return self.__engine.connect().execute(self.league_of_legends_sql_json["delete_all"]).rowcount


if __name__ == '__main__':
    oracle_connection = OracleConnection()
    oracle_connection.delete_all()
