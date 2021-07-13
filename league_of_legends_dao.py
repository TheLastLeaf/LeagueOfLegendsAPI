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

    def league_of_legends_champions_select(self, version, language, id):
        sql = sqlalchemy.text(self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_CHAMPIONS"]["SELECT"])
        row = self.__engine.connect().execute(sql, VERSION=version, LANGUAGE=language, ID=id).fetchone()
        return row

    def league_of_legends_champions_insert(self, language, champion):
        if self.league_of_legends_champions_select(champion["version"], language, champion["id"]):
            return 0

        sql = sqlalchemy.text(self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_CHAMPIONS"]["INSERT"])
        return self.__engine.connect().execute(sql,
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

    def league_of_legends_champions_insert_list(self, language, champions):
        return sum(self.league_of_legends_champions_insert(language, champion) for champion in champions)

    def league_of_legends_champions_delete_list(self, version, language):
        sql = sqlalchemy.text(self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_CHAMPIONS"]["DELETE_LIST"])
        return self.__engine.connect().execute(sql,
                                               VERSION=version,
                                               LANGUAGE=language).rowcount

    def league_of_legends_champions_delete_all(self):
        return self.__engine.connect().execute(
            self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_CHAMPIONS"]["DELETE_ALL"]).rowcount

    def league_of_legends_versions_select(self, version):
        sql = sqlalchemy.text(self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_VERSIONS"]["SELECT"])
        return self.__engine.connect().execute(sql, VERSION=version).fetchone()

    def league_of_legends_versions_insert(self, version):
        if self.league_of_legends_versions_select(version):
            return 0
        sql = sqlalchemy.text(self.league_of_legends_sql_json["LEAGUE_OF_LEGENDS_VERSIONS"]["INSERT"])
        return self.__engine.connect().execute(sql, VERSION=version).rowcount

    def league_of_legends_versions_insert_list(self, versions):
        return sum(self.league_of_legends_versions_insert(version) for version in reversed(versions))


if __name__ == '__main__':
    oracle_connection = OracleConnection()
