import json
import os

import config
import click
import pymysql


def export_transcriptions(db, export_path):
    cursor = db.cursor(pymysql.cursors.DictCursor)
    cursor.execute('''
SELECT
    CONCAT(
        DATE_FORMAT(p.post_date, '%Y/%m/%d/'),
        DATE_FORMAT(FROM_UNIXTIME(t.ID), '%Y-%m-%dT%H:%i:%s.000Z'),
        '.json'
    ) AS file_name,
    t.user,
    t.text
FROM ff_transcription t
INNER JOIN ff_posts p
ON (p.ID = t.post_id)
''')

    for row in cursor:
        file_name = os.path.join(export_path,row['file_name'])
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, mode='w') as output_file:
            json.dump(
                dict((key, row[key]) for key in ['user', 'text']),
                output_file,
                ensure_ascii=False,
                indent=4,
                sort_keys=True
            )


@click.command()
@click.argument('export_path', type=click.Path())
def run_export(export_path):
    db = pymysql.connect(**config.mysql)
    export_transcriptions(db=db, export_path=export_path)


if __name__ == '__main__':
    run_export()