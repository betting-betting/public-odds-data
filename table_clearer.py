import sql
from imp import reload

reload(sql)

from sql import sqlExecute


class table_data_mover:
    
    def __init__(self):
        pass
    
    def clearer(self):
        current_tables : list = open('tables_to_clear.txt','r').read().split('\n')
        old_tables : list = [f'{table}_old' for table in current_tables]
        
        for current_table,old_table in zip(current_tables,old_tables):
            copy_statement : str =  f"""
            insert into {old_table} 
            select *
            from
            {current_table}"""
            sqlExecute(copy_statement)
            print(f'{current_table} data copied to {old_table}')
            
            delete_statement : str = f"""
            truncate {current_table}
            """
            sqlExecute(delete_statement)
            print(f'{current_table} wiped')
            
if __name__ == '__main__':
    table_data_mover().clearer()