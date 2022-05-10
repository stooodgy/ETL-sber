import jaydebeapi
import pandas as pd
import xlrd
import os, fnmatch

#подключение к БД
conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

curs = conn.cursor()


#Формирование таблицы фактов по терминалам
#1 Функция загрузки из файла во временную таблицу
def xls_to_db_terminal(file):
	df = pd.read_excel(file)
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_terminals(
				terminal_id varchar(128),
				terminal_type varchar(128),
				terminal_city varchar(128),
				terminal_address varchar(255)
				)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции xls_to_db_terminal: ' + str(error))
	try:
		sql = '''INSERT INTO s_20_stg_terminals(terminal_id, terminal_type, terminal_city, terminal_address) 
				      VALUES (?, ?, ?, ?)'''
		curs.executemany(sql, df.values.tolist())

	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции xls_to_db_terminal: ' + str(error))

#2 Функция создания таблицы и вьюхи для терминалов
def create_terminals_hist():
	try:
		curs.execute('''
			CREATE TABLE s_20_dwh_dim_terminals_hist(
				terminal_id varchar(128),
				terminal_type varchar(128),
				terminal_city varchar(128),
				terminal_address varchar(128),
				deleted_flg integer default 0,
			    effective_from timestamp default sysdate,
				effective_to timestamp default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'))
			)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_terminals_hist: ' + str(error))
	try:
		curs.execute('''
			CREATE VIEW s_20_v_terminals_hist as (
				select
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address
				from s_20_dwh_dim_terminals_hist
				where sysdate between effective_from and effective_to
					and deleted_flg = 0
			)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_terminals_hist: ' + str(error))

#3 Функция создания временной таблицы новых данных
def create_new_rows_terminals(): 
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_n_rows_terminals as
				(select
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address
				from s_20_stg_terminals t1
				left join s_20_v_terminals_hist t2
			   	on t1.terminal_id = t2.terminal_id
				where t2.terminal_id is null)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_new_rows_terminals: ' + str(error))

#4 Функция создания временной таблицы удаленных данных
def create_del_rows_terminals():
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_d_rows_terminals as
				(select
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address	
				from s_20_v_terminals_hist t1
				left join s_20_stg_terminals t2
				on t1.terminal_id = t2.terminal_id
				where t2.terminal_id is null)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_del_rows_terminals: ' + str(error))

#5 Функция создания временной таблицы измененных данных
def create_changed_rows_terminals():
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_c_rows_terminals as
				(select
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address
				from s_20_stg_terminals t1
				inner join s_20_v_terminals_hist t2
				        on t1.terminal_id = t2.terminal_id
			         	and (t1.terminal_type <> t2.terminal_type or
					         t1.terminal_city <> t2.terminal_city or
					         t1.terminal_address <> t2.terminal_address)
				)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_changed_rows_terminals: ' + str(error))

#6 Функция добавления данных в историческую таблицу
def insert_dwh_dim_terminals():
	try:
		curs.execute('''
			UPDATE s_20_dwh_dim_terminals_hist
			set effective_to = sysdate-1/24/60/60
			where terminal_id in (select terminal_id from s_20_stg_d_rows_terminals)
			  and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		''')
		curs.execute('''
			UPDATE s_20_dwh_dim_terminals_hist
			set effective_to = sysdate-1/24/60/60
			where terminal_id in (select terminal_id from s_20_stg_c_rows_terminals)
			  and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		''')
		curs.execute('''
			INSERT INTO s_20_dwh_dim_terminals_hist(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			from s_20_stg_n_rows_terminals
		''')
		curs.execute('''
			INSERT INTO s_20_dwh_dim_terminals_hist (
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			from s_20_stg_c_rows_terminals
		''')
		curs.execute('''
			INSERT INTO s_20_dwh_dim_terminals_hist (
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				deleted_flg
			)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				1
			from s_20_stg_d_rows_terminals
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции insert_dwh_dim_terminals: ' + str(error))

#7 Функция очистки стейджинговых таблиц
def drop_stg_terminals():
	try:
		curs.execute('DROP TABLE s_20_stg_n_rows_terminals')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_terminals: ' + str(error))
	try:
		curs.execute('DROP TABLE s_20_stg_d_rows_terminals')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_terminals: ' + str(error))
	try:	
		curs.execute('DROP TABLE s_20_stg_c_rows_terminals')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_terminals: ' + str(error))
	try:
		curs.execute('DROP TABLE s_20_stg_terminals')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_terminals: ' + str(error))


#Формирование таблицы фактов по паспортам
#1 Функция загрузки из файла во временную таблицу
def xls_to_db_passport(file):
	df = pd.read_excel(file)
	df = df.reindex(columns = ['passport','date'])
	df = df.astype({'date': str})
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_passport_blacklist(
				passport_num varchar(20),
				entry_dt date)
		''')
		sql = '''INSERT INTO s_20_stg_passport_blacklist(passport_num, entry_dt) 
					  VALUES (?, to_date (?, 'YYYY-MM-DD'))'''
		curs.executemany(sql, df.values.tolist())
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции xls_to_db_passport: ' + str(error))

#2 Функция создания таблицы фактов паспортов
def create_fact_passport_blk(): 
	try:
		curs.execute('''
			CREATE TABLE s_20_dwh_fact_pssprt_blcklst(
				passport_num varchar(20),
				entry_dt date	
			)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_fact_passport_blk: ' + str(error))

#3 Функция создания таблицы новых данных
def create_new_rows_passport_blk(): 
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_n_rows_pssprt_blcklst as
				(select
					t1.passport_num,
					t1.entry_dt
				from s_20_stg_passport_blacklist t1
				   left join s_20_dwh_fact_pssprt_blcklst t2
				          on t1.passport_num = t2.passport_num
				   where t2.passport_num is null)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_new_rows_passport_blk: ' + str(error))

#4 Функция создания таблицы измененных данных
def create_change_rows_passport_blk(): 
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_c_row_pssprt_blcklst as
				(select
					t1.passport_num,
					t1.entry_dt
				from s_20_stg_passport_blacklist t1
				inner join s_20_dwh_fact_pssprt_blcklst t2
				on t1.passport_num = t2.passport_num
				and t1.entry_dt <> t2.entry_dt)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_change_rows_passport_blk: ' + str(error))

#5 Функция добавления заполнения таблицы фактов
def insert_fact_passport_blk(): 
	try:
		curs.execute('''
			INSERT INTO s_20_dwh_fact_pssprt_blcklst(
				passport_num,
				entry_dt
			)
			select
				passport_num,
				entry_dt
			from s_20_stg_n_rows_pssprt_blcklst
		''')
		curs.execute('''
			INSERT INTO s_20_dwh_fact_pssprt_blcklst(
				passport_num,
				entry_dt
			)
			select
				passport_num,
				entry_dt
			from s_20_stg_c_row_pssprt_blcklst
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции insert_fact_passport_blk: ' + str(error))

#6 Функция очистки стейджинговых таблиц
def drop_stg_passport_blk():
	try:
		curs.execute('DROP TABLE s_20_stg_n_rows_pssprt_blcklst')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_passport_blk: ' + str(error))
	try:
		curs.execute('DROP TABLE s_20_stg_c_row_pssprt_blcklst')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_passport_blk: ' + str(error))
	try:
		curs.execute('DROP TABLE s_20_stg_passport_blacklist')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_passport_blk: ' + str(error))


#Формирование таблицы фактов по транзакциям
#1 Функция загрузки из файла во временную таблицу
def csv_to_db_transaction(file):
	df = pd.read_csv(file, sep=';')
	df = df.astype({'transaction_date': str})
	try:
		curs.execute('''
			CREATE TABLE s_20_stg_transaction(
				trans_id varchar(128),
				trans_date varchar(128),
				amt number,
				card_num varchar(128),
				oper_type varchar(128),
				oper_result varchar(128),
				terminal varchar (128)
			)
		''')
		sql = '''INSERT INTO s_20_stg_transaction (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) 
				      values (?, ?, ?, ?, ?, ?, ?)'''
		curs.executemany(sql, df.values.tolist())
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции csv_to_db_transaction: ' + str(error))

#2 Функция создания таблицы транзакций
def create_fact_transactions():
	try:
		curs.execute('''
			CREATE TABLE s_20_dwh_fact_transactions(
				trans_id varchar(15),
				trans_date timestamp (6),
				card_num varchar(128),
				oper_type varchar(50),
				amt decimal (*,2),
				oper_result varchar(50),
				terminal varchar (15),
				create_date timestamp default sysdate
			)
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_fact_transactions: ' + str(error))

#3 Функция заполнения таблицы транзакций
def insert_fact_transactions():
	try:
		curs.execute('''
			INSERT INTO s_20_dwh_fact_transactions(
				trans_id,
				trans_date,
				card_num,
				oper_type,
				amt,
				oper_result,
				terminal
			)
			select
				trans_id,
				to_timestamp(trim(trans_date), 'YYYY-MM-DD HH24:MI:SS'),
				card_num,
				oper_type,
				amt,
				oper_result,
				terminal
			from s_20_stg_transaction
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции insert_fact_transactions: ' + str(error))

#4 Функция очистки стейджинговых таблиц
def drop_stg_transactions():
	try:
		curs.execute('DROP TABLE s_20_stg_transaction')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции drop_stg_transactions: ' + str(error))


#Функция создания представления по существующим в БД таблицам
def create_stg_data_view():
	try:
		curs.execute('''
			CREATE OR REPLACE VIEW s_20_stg_data_view as (
                select
					cl.client_id,
					cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as name,
					cl.passport_num,
					cl.passport_valid_to,
					trim(crd.card_num) as card_num,
					cl.phone,
                    acc.account,
					acc.valid_to,
					trs.trans_id,
					trs.trans_date,
					trs.amt,
					trs.oper_result,
					trm.terminal_type,
					trm.terminal_city
				from 
					bank.clients cl
					   inner join bank.accounts acc
					           on cl.client_id = acc.client
					   inner join bank.cards crd 
					           on acc.account = crd.account
					   inner join s_20_dwh_fact_transactions trs
					           on trim(crd.card_num) = trs.card_num
					          and trunc(create_date) = trunc(sysdate)
					   inner join s_20_dwh_dim_terminals_hist trm
					           on trs.terminal = trm.terminal_id
			    )
		''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_stg_data_view: ' + str(error))


#Функция создания витрины
def create_rep_fraud():
	try:
		curs.execute('''
				CREATE TABLE s_20_rep_fraud
				(
					event_dt timestamp (6),
					passport varchar(128),
					fio varchar(128),
					phone varchar (128),
					event_type varchar(255),
					report_dt date default sysdate
				)
			''')
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции create_rep_fraud: ' + str(error))


#Функция наполнения витрины
def insert_rep_fraud():
	try:
		curs.execute('''
				INSERT INTO s_20_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
				select
					event_dt,
					passport_num,
					name,
					phone,
					'Совершение операции при просроченном или заблокированном паспорте',
					sysdate
				from (	
                    select
                        distinct
						t1.event_dt,
						t2.passport_num,
						t2.name,
						t2.phone
					from 
						(select
							v.client_id,
							v.trans_date as event_dt
						from s_20_dwh_fact_pssprt_blcklst bl
							left join s_20_stg_data_view v
                                   on bl.passport_num = v.passport_num
                                  and bl.entry_dt <= trunc(v.trans_date)
                                   or v.passport_valid_to < trunc(v.trans_date)
						group by v.client_id, v.trans_date) t1
					inner join s_20_stg_data_view t2
					        on t1.client_id = t2.client_id
				)
		''')
		curs.execute('''
				INSERT INTO s_20_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
				select
					trans_date,
					passport_num,
					name,
					phone,
					'Совершение операции при недействующем договоре',
					sysdate
				from s_20_stg_data_view
				where trunc (trans_date) > valid_to
		''')
		curs.execute('''
				INSERT INTO s_20_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
				select
					trans_date,
					passport_num,
					name,
					phone,
					'Совершение операций в разных городах в течение одного часа',
					sysdate
				from
					(select distinct
							t2.trans_date as trans_date,
							passport_num,
							name,
							phone,
							t2.terminal_city,
							lead(t2.terminal_city, 1) over (partition by name order by t2.trans_date) as next_terminal_city,
							dense_rank() over(partition by name order by t2.terminal_city) as rank
						from
							(select 
									card_num,
									terminal_city,
									trans_date,
									passport_num,
									name,
									phone
							from s_20_stg_data_view) t1
						inner join 
							(select 
									card_num,
									terminal_city,
									trans_date
							from s_20_stg_data_view) t2
						on t1.card_num = t2.card_num
						and t1.terminal_city != t2.terminal_city
						and t1.trans_date ! = t2.trans_date
					where t1.trans_date between t2.trans_date-1/24 and t2.trans_date+1/24) t5              
				where t5.next_terminal_city is not null
				and t5.terminal_city != next_terminal_city
				and t5.rank = 2
		''')		
	except jaydebeapi.DatabaseError as error:
		print('Ошибка в функции insert_rep_fraud: ' + str(error))


#Функция перемещения файлов в архив
def move_file_to_backup(file):
	try:
		os.rename(file,'Archive/'+file+'.backup')
		print('Файл успешно перемещен в архив')
	except:
		print('Не удалось переместить файл ' + file + ' в архив!')


#Функция для обработки файлов в директории
def files_load():
	list_files = os.listdir()
	for file_name in list_files:
		if fnmatch.fnmatch(file_name, 'passport*'):
			drop_stg_passport_blk()	
			create_fact_passport_blk()		
			xls_to_db_passport(file_name)
			create_new_rows_passport_blk()
			create_change_rows_passport_blk()
			insert_fact_passport_blk()
			move_file_to_backup(file_name)
			drop_stg_passport_blk()	
		elif fnmatch.fnmatch(file_name, 'terminals*'):
			drop_stg_terminals()
			create_terminals_hist()
			xls_to_db_terminal(file_name)
			create_new_rows_terminals()
			create_del_rows_terminals()
			create_changed_rows_terminals()
			insert_dwh_dim_terminals()
			move_file_to_backup(file_name)
			drop_stg_terminals()
		elif fnmatch.fnmatch(file_name, 'transactions*'):
			drop_stg_transactions()
			create_fact_transactions()
			csv_to_db_transaction(file_name)
			insert_fact_transactions()
			move_file_to_backup(file_name)
			drop_stg_transactions()

	create_stg_data_view()
	create_rep_fraud()
	insert_rep_fraud()


#Запускаем весь процесс
files_load()