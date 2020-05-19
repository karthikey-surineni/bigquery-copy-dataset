import yaml
import os
import logging

log = logging.getLogger(__file__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
DBT_PATH = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "models"))
DBT_TEMPLATE = {'version': 2, 'sources': None}
# 'models' : None
DBT_SCHEMA_FILE = "schema.yml"
TEST_SOURCES = [{'name':'test_AU','tables':[{'name':'incidents_2008'},{'name':'incidents_2008'}]}]
TEST_MODELS = [{'name':'test_AU','tables':[{'name':'incidents_2008'},{'name':'incidents_2008'}]}]
class DBTConfig():
    def __init__(self,path,dbt_config=DBT_TEMPLATE):
        self.__dbt_config = dbt_config
        self.__path = os.path.join(DBT_PATH,path)
        self.write_config()
        super().__init__()

    @property
    def dbt_config(self):
        return self.__dbt_config

    def write_config(self):
        if not os.path.isdir(os.path.join(self.__path)):
            os.mkdir(self.__path)
        with open(os.path.join(self.__path,DBT_SCHEMA_FILE), 'w', encoding='utf8') as yaml_schema:
            yaml.dump(self.__dbt_config, yaml_schema)

    def read_config(self):
        with open(os.path.join(self.__path,DBT_SCHEMA_FILE),'r') as file:
            self.__dbt_config = yaml.load(file, Loader=yaml.Loader)

    def write_sources(self,source_dict=TEST_SOURCES):
        self.__dbt_config['sources'] = source_dict

    def write_models(self,model_dict=TEST_MODELS):
        self.__dbt_config['model'] = model_dict

    def write_model_sql(self,model_name,model_query):
        with open(os.path.join(self.__path,f'{model_name}.sql'), 'w', encoding='utf8') as model_sql:
            model_sql.write(model_query)

# SAMPLE USAGE
# if __name__ == "__main__":
#     dbt = DBTConfig(path='/Users/karthikeysurineni/bigquery-copy-dataset/src/dbt/models/test_AU')
#     dbt.read_config()
#     print(dbt.dbt_config)
#     dbt.write_sources()
#     print(dbt.dbt_config)
#     dbt.write_config()