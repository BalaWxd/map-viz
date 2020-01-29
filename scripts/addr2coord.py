# -*- coding: UTF-8 -*-
import os, json
import pandas as pd
import requests


class Addr2Coord:
    """A simple address to coordinate class uses Baidu Map API wrapped."""

    data_base_dir = '/home/ubuntu/projects/code/api-server/wuhan2020/data/json'
    bmap_parser_base_uri = 'https://service-qf7o2c4u-1252957949.gz.apigw.tencentcs.com/release/bmap?address='
    output_file = '/home/ubuntu/projects/code/transfomers/data/coordinates_mock_data.json'

    HOSPITAL_COLUMNS = ['医院名称', '医院地址']
    HOTEL_COLUMNS = ['酒店名称', '酒店地址']
    COLUMNS = { '医院': HOSPITAL_COLUMNS, '宾馆': HOTEL_COLUMNS }

    COORDINATABLES_CATALOG_DIR = ['医院', '宾馆']

    def get_data_files(self):
        """Get data files in provided directoy"""

        data_files = {}
        for catalog in [catalog_dir for catalog_dir in os.listdir(self.data_base_dir)]:
            if catalog in self.COORDINATABLES_CATALOG_DIR:
                data_files[catalog] = [filename for filename in os.listdir(os.path.join(self.data_base_dir, catalog)) if filename.endswith('.json')]

        return data_files

    def get_coord_for_addresses(self, data_files, catalog):
        """Load the data files contains addresses and get coordnate for each"""

        result = []
        for index, data_file in enumerate(data_files):
            row = {}
            with open(os.path.join(self.data_base_dir, catalog, data_file), 'r') as json_file:
                city = data_file.rsplit('.', 1)[0]
                items = json.load(json_file)
                data_frame = pd.DataFrame(pd.io.json.json_normalize(items), columns=self.COLUMNS[catalog])

                for index, dict_data in data_frame.iterrows():
                    data = dict_data.tolist()
                    response = self.get_coord(data[1], city)
                    content = json.loads(response.text)
                    row['coord'] = content['data']['result']
                    row['address'] = data[1]
                    row['city'] = city
                    row['name'] = data[0]
                    row['type'] = catalog
                    result.append(row)

        return result

    def get_coord(self, address, city = ''):
        """Request bmap api to get the address coordinate"""

        search_uri = self.bmap_parser_base_uri + address
        if (city):
            search_uri = search_uri + '&city='+city

        return requests.get(search_uri)


    def run(self):
        results = []
        data_files = self.get_data_files()
        for catalog in self.COORDINATABLES_CATALOG_DIR:
            results.extend(self.get_coord_for_addresses(data_files[catalog], catalog))

        with open(self.output_file, 'w') as f:
            json.dump(results, f)


instance = Addr2Coord()
instance.run()
