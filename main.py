import sqlite3
import pandas as pd
import os

def create_sqlite_db(file_name):
    con = sqlite3.connect(file_name+".sqlite")
    f_dump = open(file_name+".db",'r',encoding='utf-8-sig')
    dump=f_dump.read()
    f_dump.close()
    con.executescript(dump)
    con.commit()
    con.close()

if not os.path.isfile("library.sqlite"):
    create_sqlite_db("library")

if not os.path.isfile("store.sqlite"):
    create_sqlite_db("store")

if not os.path.isfile("booking.sqlite"):
    create_sqlite_db("booking")


con = sqlite3.connect("store.sqlite")
cursor = con.cursor()


#task 1
df = pd.read_sql('''
            select 
                title as "Название", 
                name_author as "Автор",
                amount as "Количество",
                IIF(book.amount<3,'мало',
                    IIF(book.amount>10,'много','достаточно')) as "Наличие"
                from book
                join author using (author_id)
                order by 3 asc, 2 desc
            ''',con)


print(df)

#task 2
df=pd.read_sql('''
            SELECT
                name_step as "Этап",
                ceil(avg(               
                JulianDay(date_step_end) - JulianDay(date_step_beg)
                )+1)
                as "Среднее время"
                from step 
            join buy_step using (step_id)
            where date_step_beg is not null
                group by step_id
            order by 2 desc, 1 asc
            ''',con)

print(df)

#task 3
df = pd.read_sql('''
                with MaxSells as
                (
                select max(cnt) as max_sells from 
                (select count(amount) as cnt, buy_book.book_id
                from buy_book
                group by book_id)
                )
                
                select 
                title as 'Название',
                name_author as 'Автор', 
                count(buy_book.amount) as 'Количество продаж' 
                from MaxSells, buy_book
                join book using (book_id)
                join author using (author_id)
                group by buy_book.book_id
                having count(buy_book.amount) = max_sells
                ''',con)

print(df)

#task 4
print('До изменения цен:')
df=pd.read_sql('''
            select * from book               
            ''',con)
print(df)

cursor.executescript('''
                with AVERAGE_COUNT as
                    (
                    select 
                    sum(amount)/count(amount) as av_cnt 
                    from buy_book
                    ),

                    BHA as 
                    (
                        select book_id from buy_book 
                        join AVERAGE_COUNT as AC
                        group by book_id                        
                        having sum(amount)>AC.av_cnt
                    )
                    
                UPDATE book
                set price=
                    CASE
                        when book_id in BHA then price*'1.1'
                        else price*'0.95'
                    END
            ''')

con.commit()

print('После изменения цен:')
df=pd.read_sql('''
            select * from book               
            ''',con)
print(df)


create_sqlite_db("store")

#task 5
df=pd.read_sql('''
                select
                    name_author as "Автор",
                    title as "Название",
                    price as "Цена",
                    count(price) over mywindow as Количество
                    from book                    
                    join author using (author_id)
                    WINDOW mywindow
                    AS (
                        PARTITION BY author_id
                        ORDER BY price
                        RANGE BETWEEN 40 PRECEDING AND 40 FOLLOWING 
                    )            
                    order by 1, 3
               ''',con)
print(df)

con.close()