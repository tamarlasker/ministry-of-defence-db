from dataclasses import dataclass
from pathlib import Path
from sys import path
from typing import Any, Dict, List, Type
import csv
import os #for deleting file
from dataclasses_json import dataclass_json
import json
import db_api
DB_ROOT = Path('db_files')

probas = ['80%-100%,', '60%-80%,','40%-60%', '20%-40%', '0%-20%']

############   index funcs   ############

def add_new_index(table, probability):
    with (DB_ROOT/f'index_{table["table_name"]}_{probability}.json').open('w') as file:
        json.dump({}, file)
    return file.name


def add_key_to_index(index_file, key, file):
    with open(index_file, 'r') as json_file:
        index_data = json.load(json_file)
    index_data[key] = file
    with open(index_file, 'w') as json_file:
        json.dump(index_data, json_file)


def get_relevant_file_from_index(index_file, key):
    with open(index_file, 'r') as json_file:
        index_data = json.load(json_file)
    return index_data[key]


def delete_item_from_index(key, index_file):
    with open(index_file, 'r') as json_file:
        index_data = json.load(json_file)
    del index_data[key]

    with open(index_file, 'w') as json_file:
        json.dump(index_data, json_file)

########################################################

def get_csv_file_as_list(file_name):
    with open(file_name, 'r') as file:
        csvFile = csv.reader(file)
        all_records = [record for record in csvFile]
    return all_records


def add_new_file(table_name, probability, serial):
    with (DB_ROOT/f'{table_name}_{probability}_{serial}.csv').open('w') as file:
        pass
    return file.name


def add_record_to_file(line, file_name):
    with open(file_name, 'a', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerow(line)


def insert_new_record(fields, values: Dict[str, Any], file):
    new_record = []
    try:
        values.pop('probability')
    except KeyError:
        pass
    for field in fields:
        item = values[DBField(field, int).name] if DBField(field, int).name in values.keys() else None
        new_record.append(item)
    add_record_to_file(new_record, file)


def find_table(table_name: str):
    with (DB_ROOT/'metadata.json').open('r') as file:
        all_tables = json.load(file)
        for t in all_tables.values():
            if t['table_name'] == table_name:
                return t


def num_of_rows_in_file(file_name):
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        return reader.line_num


def get_metadate():
    with open(DB_ROOT/'metadata.json', 'r') as file:
        data = json.load(file)
    return data


def convert_table_to_dict(table):
    return {'table_name': table.name, 'fields': [t.name for t in table.fields], 'primary_key': table.key_field_name,
         'counter':table.counter, 'probability': {}}


def update_metadata(table):
    data = get_metadate()
    for elem in data.values():
        if elem['table_name'] == table['table_name']:
            elem['counter'] += 1
            elem['probability'] = table['probability']
            break
    with (DB_ROOT/'metadata.json').open('w') as file:
        json.dump(data, file)



@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    # name: str
    # fields: List[db_api.DBField]
    # key_field_name: str

    def __init__(self, table_name: str, fields: List[db_api.DBField], key_field_name: str, counter = 0):
        self.name = table_name
        self.fields = fields
        self.key_field_name = key_field_name
        self.counter = counter

    def count(self):
        return find_table(self.name)['counter']

    def insert_record(self, values: Dict[str, Any]):#TODO no duplications of primary key, implement by index.
        my_table = find_table(self.name)
        record_proba = values['probability'] if 'probability' in values.keys() else '40% - 60%'
        try:
            files = my_table['probability'][record_proba]
        except KeyError:
            my_table['probability'][record_proba] = [add_new_file(my_table['table_name'], record_proba, 0)]
            my_table['probability'][record_proba].insert(0, add_new_index(my_table, record_proba))
            files = my_table['probability'][record_proba]

        if num_of_rows_in_file(files[-1]) > 1000:
            files.append(add_new_file(my_table['table_name'], record_proba, len(files)))
        insert_new_record(self.fields, values, files[-1])
        add_key_to_index(files[0], values[my_table['primary_key']], files[-1])
        update_metadata(my_table)

    def delete_record(self, key: Any):
        meta_table = find_table(self.name)
        for proba in probas:
            index_file = meta_table['probability'][proba]
            try:
                file = get_relevant_file_from_index(key, index_file)
            except KeyError:
                continue

            all_records = get_csv_file_as_list(file)
            rec_to_delete = [rec for rec in all_records if rec[0]==1]
            all_records.remove(rec_to_delete)
            with open(file, 'w') as f:
                writer = csv.writer(f)
                writer.writerows(all_records)
            delete_item_from_index(key, index_file)

    # def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:

        

    # def get_record(self, key: Any) -> Dict[str, Any]:
    #     files = find_table(self.name)[-1]


    # def update_record(self, key: Any, values: Dict[str, Any]) -> None:
    #     raise NotImplementedError
    #
    # def query_table(self, criteria: List[SelectionCriteria]) \
    #         -> List[Dict[str, Any]]:
    #     raise NotImplementedError
    #
    # def create_index(self, field_to_index: str) -> None:
    #     raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):

    def __init__(self):
        if not os.path.isfile(DB_ROOT/"metadata.json"):
            d = {}
            with(DB_ROOT/"metadata.json").open('w') as file:
                json.dump(d, file)

    def num_tables(self):
        with (DB_ROOT/'metadata.json').open('r') as file:
            return len(json.load(file))

    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str):#TODO validation
        t = DBTable(table_name, fields, key_field_name)
        data = get_metadate()
        if t.name in [table['table_name'] for table in data.values()]:
            raise Exception("AlreadyExistTable")
        data[f'{self.num_tables()+1}'] = convert_table_to_dict(t)
        with (DB_ROOT/'metadata.json').open('w') as file:
            json.dump(data, file)
        return t

    def get_table(self, table_name: str):
        table = find_table(table_name)
        return DBTable(table['table_name'], table['fields'], table['primary_key'], table['counter'])

    def delete_table(self, table_name: str):
        table = find_table(table_name)
        for files in table['probability'].values():
            for file in files:
                os.remove(file)
        all_tables = get_metadate()
        all_tables = {key: val for key, val in all_tables.items() if val != find_table(table_name)}
        with (DB_ROOT/'metadata.json').open('w') as file:
            json.dump(all_tables, file)

    def get_tables_names(self):
        return [t['table_name'] for t in get_metadate().values()]

    #
    # def query_multiple_tables(
    #         self,
    #         tables: List[str],
    #         fields_and_values_list: List[List[db_api.SelectionCriteria]],
    #         fields_to_join_by: List[str]
    # ) -> List[Dict[str, Any]]:
    #     raise NotImplementedError
