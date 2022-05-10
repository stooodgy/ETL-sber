import jaydebeapi
import pandas as pd

def xlsxtosql(path,tablename):
	df=pd.read_excel(path)
	print(df.head())
	df.xlsxtosql(name=tablename, con=connect, if_exists='replace')

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')
curs = conn.cursor()

curs.execute('select * from bank.transactions')

for row in curs.fetchall():
	print(row)


cursor.execute('''
			CREATE VIEW v_DE3NH_S_30_DWH_FACT_TRANSACTIONS as
				select
					transaction_id,
					transaction_date,
		    		amount,
					card_num,
					oper_type,
					oper_result,
					terminal,
					create_dt,
					update_dt
				from DE3NH_S_30_DWH_FACT_TRANSACTIONS
				where create_dt > (current_timestamp-2)
				''')		


cursor.execute('''
		CREATE VIEW v_DE3NH_S_30_DWH_FACT_PASSPORT_BLACKLIST as
			select
				passport,
				entry_dt,
		    	create_dt,
				update_dt
			from DE3NH_S_30_DWH_DIM_PASSPORT_BLACKLIST 
			where create_dt > (current_timestamp-2)
			''')		

cursor.execute('''
		CREATE VIEW v_DE3NH_S_30_DWH_FACT_TERMINALS as
			select
				terminal_id,
				terminal_type,
		    	terminal_city,
				terminal_adress,
				deleted_flg,
				effective_from_dttm,
				effective_to_dttm
			from DE3NH_S_30_DWH_DIM_TERMINALS_HIST 
			where current_timestamp between effective_from_dttm and effective_to_dttm
			and deleted_flg=0
		''')