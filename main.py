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
                book.title as "Название", 
                author.name_author as "Автор",
                book.amount as "Количество",
                IIF(book.amount<3,'мало',
                    IIF(book.amount>10,'много','достаточно')) as "Наличие"
                from book, author
                where book.author_id=author.author_id
                 order by 3 asc, 2 desc
               ''',con)


print(df)

#task 2
df=pd.read_sql('''
               SELECT
                step.name_step as "Этап",
                ceil(avg(
               Cast ((
                JulianDay(buy_step.date_step_end) - JulianDay(buy_step.date_step_beg)
                )+1 As Integer)
               )) as "Среднее время"
                from step, buy_step
                where buy_step.date_step_end is not Null and step.step_id=buy_step.step_id
                group by step.step_id
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
                 
                select b.title as 'Название', a.name_author as 'Автор', count(buy_book.amount) 
                 as 'Количество продаж' from buy_book  
                 join book as b on b.book_id=buy_book.book_id
                 join author as a on a.author_id = b.author_id 
                 join MaxSells as MS
                 group by buy_book.book_id
                 having count(buy_book.amount) = MS.max_sells
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
                        group by buy_book.book_id                        
                        having sum(buy_book.amount)>AC.av_cnt
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




#task 5
df=pd.read_sql('''
               with Diffs as(
               select book.book_id as first_id, book2.book_id as second_id,
               abs(book.price-book2.price) as diff_price
               from book
               join book as book2 on book2.book_id != book.book_id and book.author_id=book2.author_id)

                select
                    book.title as "Название",
                    author.name_author as "Автор",
                    book.price as "Цена",
                    count(*) as "Количество"
                    from book
                    join Diffs on Diffs.first_id=book.book_id and diff_price<=40
                    join author on author.author_id=book.author_id
                    group by book.book_id               
               ''',con)
print(df)

con.close()