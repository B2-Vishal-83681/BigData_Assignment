1)
CREATE TABLE sratings(
    userId INT, movieId int, rating int,rtime BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')

select t1.movieid m1,t2.movieid m2,t1.rating r1,t2.rating r2 from sratings t1 inner join sratings t2 on t1.userid=t2.userid where t1.movieid<t2.movieid;

create table cor_movies as 
select t1.movieid m1,t2.movieid m2,t1.rating r1,t2.rating r2 from sratings t1 inner join sratings t2 on t1.userid=t2.userid where t1.movieid<t2.movieid;

create table cor_table as
select m1,m2,corr(r1,r2) corm from cor_movies 
group by m1,m2
having corm is not null;





CREATE TABLE ratings_staging(
    userId INT, movieId int, rating int,rtime BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')


LOAD DATA LOCAL INPATH '/home/sunbeam/pracBigData/movies/ratings.csv' INTO TABLE ratings_staging;

SELECT * FROM ratings_staging;



CREATE TABLE ratings(
    userId INT,
    movieId INT,
    rating DOUBLE,
    rtime TIMESTAMP
)
STORED AS ORC
TBLPROPERTIES('transactional'='true');


INSERT INTO ratings
select userId, movieId, rating, FROM_UNIXTIME(rtime) from ratings_staging;

SELECT * FROM ratings LIMIT 5;

SELECT YEAR(rtime) yr, COUNT(*) cnt FROM ratings
GROUP BY YEAR(rtime);



CREATE TABLE movies_staging(
    movieId int,
    title STRING,
    genres STRING
)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    'separatorChar'=',',
    'quoteChar'='"',
    'escapeChar'='\\'
)
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/home/sunbeam/pracBigData/movies/movies.csv' INTO TABLE movies_staging;


CREATE TABLE movies(
    movieId int,
    title STRING,
    genres ARRAY<STRING>
)
STORED AS ORC
TBLPROPERTIES('transactional'='true');


INSERT INTO movies
SELECT movieId,title,SPLIT(genres,'\\|') from movies_staging;

SELECT * FROM  movies
LIMIT 15;



create table cor_movies as 
select t1.movieid m1,t2.movieid m2,t1.rating r1,t2.rating r2 from ratings t1 inner join ratings t2 on t1.userid=t2.userid where t1.movieid<t2.movieid;



create table cor_table as
select m1,m2,corr(r1,r2) corm from cor_movies 
group by m1,m2
having corm is not null;



select m1,(select title from movies where m1=movieId), 
m2,(select title from movies where m2=movieId),
corm from cor_table;


select m1,(select title from movies where m1=movieId), 
m2,(select title from movies where m2=movieId),
corm from cor_table where m1=142488;

+---------+-------------------+---------+--------------------------------------+----------------------+
|   m1    |        _c1        |   m2    |                 _c3                  |         corm         |
+---------+-------------------+---------+--------------------------------------+----------------------+
| 142488  | Spotlight (2015)  | 146656  | Creed (2015)                         | 0.49999999999999994  |
| 142488  | Spotlight (2015)  | 152081  | Zootopia (2016)                      | 0.9819805060619655   |
| 142488  | Spotlight (2015)  | 143385  | Bridge of Spies (2015)               | 0.9999999999999999   |
| 142488  | Spotlight (2015)  | 148626  | Big Short, The (2015)                | 0.6201736729460422   |
| 142488  | Spotlight (2015)  | 152077  | 10 Cloverfield Lane (2016)           | 0.8660254037844385   |
| 142488  | Spotlight (2015)  | 156609  | Neighbors 2: Sorority Rising (2016)  | 0.9999999999999999   |
| 142488  | Spotlight (2015)  | 157296  | Finding Dory (2016)                  | 0.9999999999999999   |
+---------+-------------------+---------+--------------------------------------+----------------------+






QUES 2)

1)
CREATE TABLE emp_staging(
empno INT,
ename STRING,
job STRING,
mgr INT,
hire DATE,
sal DOUBLE,
comm DOUBLE,
deptno INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH '/home/sunbeam/pracBigData/emp.csv' INTO TABLE emp_staging;

2)
CREATE TABLE dept_staging(
    deptno INT,
    dname STRING,
    loc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/sunbeam/pracBigData/dept.csv' INTO TABLE dept_staging;


3)
select d.dname,count(e.empno) count_emp from dept_staging d inner join emp_staging e on d.deptno=e.deptno group by d.dname;

+-------------+------------+
|   d.dname   | count_emp  |
+-------------+------------+
| ACCOUNTING  | 3          |
| RESEARCH    | 5          |
| SALES       | 6          |
+-------------+------------+

4)
select e.ename,d.dname from emp_staging e inner join dept_staging d on d.deptno=e.deptno;

+----------+-------------+
| e.ename  |   d.dname   |
+----------+-------------+
| SMITH    | RESEARCH    |
| ALLEN    | SALES       |
| WARD     | SALES       |
| JONES    | RESEARCH    |
| MARTIN   | SALES       |
| BLAKE    | SALES       |
| CLARK    | ACCOUNTING  |
| SCOTT    | RESEARCH    |
| KING     | ACCOUNTING  |
| TURNER   | SALES       |
| ADAMS    | RESEARCH    |
| JAMES    | SALES       |
| FORD     | RESEARCH    |
| MILLER   | ACCOUNTING  |
+----------+-------------+


5)
select e1.ename,e1.job,e1.deptno, e2.ename,e2.job,e2.deptno from emp_staging e1 inner join emp_staging e2 on e1.mgr=e2.empno where e1.deptno <> e2.deptno; 

+-----------+----------+------------+-----------+------------+------------+
| e1.ename  |  e1.job  | e1.deptno  | e2.ename  |   e2.job   | e2.deptno  |
+-----------+----------+------------+-----------+------------+------------+
| JONES     | MANAGER  | 20         | KING      | PRESIDENT  | 10         |
| BLAKE     | MANAGER  | 30         | KING      | PRESIDENT  | 10         |
+-----------+----------+------------+-----------+------------+------------+


6. Display all manager names with list of all dept names (where they can work).

select e1.ename,d.dname from emp_staging e1 inner join dept_staging d on e1.deptno=d.deptno and e1.job='MANAGER';

+-----------+-------------+
| e1.ename  |   d.dname   |
+-----------+-------------+
| JONES     | RESEARCH    |
| BLAKE     | SALES       |
| CLARK     | ACCOUNTING  |
+-----------+-------------+


7. Display job-wise total salary along with total salary of all employees.

select job,sum(sal) from emp_staging group by job with rollup;
+------------+----------+
|    job     |   _c1    |
+------------+----------+
| NULL       | 29025.0  |
| ANALYST    | 6000.0   |
| CLERK      | 4150.0   |
| MANAGER    | 8275.0   |
| PRESIDENT  | 5000.0   |
| SALESMAN   | 5600.0   |
+------------+----------+

8. Display dept-wise total salary along with total salary of all employees.

select deptno,sum(sal) from emp_staging group by deptno with rollup;

+---------+----------+
| deptno  |   _c1    |
+---------+----------+
| NULL    | 29025.0  |
| 10      | 8750.0   |
| 20      | 10875.0  |
| 30      | 9400.0   |
+---------+----------+

9. Display per dept job-wise total salary along with total salary of all employees.

select deptno,job,sum(sal) from emp_staging group by deptno,job with rollup;


+---------+------------+----------+
| deptno  |    job     |   _c2    |
+---------+------------+----------+
| NULL    | NULL       | 29025.0  |
| 10      | NULL       | 8750.0   |
| 20      | NULL       | 10875.0  |
| 30      | NULL       | 9400.0   |
| 20      | ANALYST    | 6000.0   |
| 10      | CLERK      | 1300.0   |
| 20      | CLERK      | 1900.0   |
| 30      | CLERK      | 950.0    |
| 10      | MANAGER    | 2450.0   |
| 20      | MANAGER    | 2975.0   |
| 30      | MANAGER    | 2850.0   |
| 10      | PRESIDENT  | 5000.0   |
| 30      | SALESMAN   | 5600.0   |
+---------+------------+----------+

10. Display number of employees recruited per year in descending order of employee count.

select year(hire),count(empno) cn from emp_staging group by year(hire) order by cn desc;

+-------+-----+
|  _c0  | cn  |
+-------+-----+
| 1981  | 10  |
| 1982  | 2   |
| 1983  | 1   |
| 1980  | 1   |
+-------+-----+


11. Display unique job roles who gets commission.

select distinct job from emp_staging where comm is not null;

+-----------+
|    job    |
+-----------+
| SALESMAN  |
+-----------+


12. Display dept name in which there is no employee (using sub-query)

select d.dname,count(e.empno) from dept_staging d left join emp_staging e on e.deptno=d.deptno group by d.dname having count(empno)=0;

select d.dname from dept_staging d where d.deptno not in (select deptno from emp_staging);

+-------------+------+
|   d.dname   | _c1  |
+-------------+------+
| OPERATIONS  | 0    |
+-------------+------+


13. Display emp-name, dept-name, salary, total salary of that dept (using sub-query).

select e.ename,(select d.dname from dept_staging d where d.deptno=e.deptno) deptname,e.sal salary, (select sum(e2.sal) from emp_staging e2 where e.deptno=e2.deptno) total_sal_dept from emp_staging e;


+----------+-------------+---------+-----------------+
| e.ename  |  deptname   | salary  | total_sal_dept  |
+----------+-------------+---------+-----------------+
| SMITH    | RESEARCH    | 800.0   | 10875.0         |
| ALLEN    | SALES       | 1600.0  | 9400.0          |
| WARD     | SALES       | 1250.0  | 9400.0          |
| JONES    | RESEARCH    | 2975.0  | 10875.0         |
| MARTIN   | SALES       | 1250.0  | 9400.0          |
| BLAKE    | SALES       | 2850.0  | 9400.0          |
| CLARK    | ACCOUNTING  | 2450.0  | 8750.0          |
| SCOTT    | RESEARCH    | 3000.0  | 10875.0         |
| KING     | ACCOUNTING  | 5000.0  | 8750.0          |
| TURNER   | SALES       | 1500.0  | 9400.0          |
| ADAMS    | RESEARCH    | 1100.0  | 10875.0         |
| JAMES    | SALES       | 950.0   | 9400.0          |
| FORD     | RESEARCH    | 3000.0  | 10875.0         |
| MILLER   | ACCOUNTING  | 1300.0  | 8750.0          |
+----------+-------------+---------+-----------------+

select d.dname,(select sum(sal) from emp_staging e where d.dname=e.dname) from dept_staging d;
+-------------+----------+
|   d.dname   |   _c1    |
+-------------+----------+
| ACCOUNTING  | 8750.0   |
| RESEARCH    | 10875.0  |
| SALES       | 9400.0   |
| OPERATIONS  | NULL     |
+-------------+----------+

14. Display all managers and presidents along with number of (immediate) subbordinates.

select e1.ename,count(e2.empno) cn from emp_staging e1 left join emp_staging e2 on e1.empno=e2.mgr WHERE e1.job in ('MANAGER','PRESIDENT') group by e1.ename,e1.empno,e1.mgr having cn<>0;

+-----------+-----+
| e1.ename  | cn  |
+-----------+-----+
| BLAKE     | 5   |
| CLARK     | 1   |
| JONES     | 2   |
| KING      | 3   |
+-----------+-----+

select e1.ename,count(e2.empno) cn from emp_staging e1 left join emp_staging e2 on e1.empno=e2.mgr WHERE e1.job in ('MANAGER','PRESIDENT') group by e1.ename having cn<>0;



3. Execute following queries on "emp" and "dept" dataset using CTE.
1. Find emp with max sal of each dept.

with cte as(
    select deptno,max(sal) msal from emp_staging group by deptno
)
select e.ename,cte.msal from emp_staging e inner join cte on e.deptno=cte.deptno where e.sal=cte.msal and e.deptno=cte.deptno;

+----------+-----------+
| e.ename  | cte.msal  |
+----------+-----------+
| KING     | 5000.0    |
| SCOTT    | 3000.0    |
| FORD     | 3000.0    |
| BLAKE    | 2850.0    |
+----------+-----------+


2. Find avg of deptwise total sal.

with cte as (
    select deptno,avg(sal) asal from emp_staging group by deptno
)
select d.dname,cte.asal from cte inner join dept_staging d on cte.deptno=d.deptno;


+-------------+---------------------+
|   d.dname   |      cte.asal       |
+-------------+---------------------+
| ACCOUNTING  | 2916.6666666666665  |
| RESEARCH    | 2175.0              |
| SALES       | 1566.6666666666667  |
+-------------+---------------------+



3. Compare (show side-by-side) sal of each emp with avg sal in his dept and avg sal for his job.


with cte as (
    select deptno,avg(sal) asal from emp_staging group by deptno
),
cte2 as(
    select job,avg(sal) asal2 from emp_staging group by job
)
select e.ename,cte.asal,cte2.asal2 from emp_staging e inner join cte on e.deptno=cte.deptno inner join cte2 on cte2.job=e.job;

+----------+---------------------+---------------------+
| e.ename  |      cte.asal       |     cte2.asal2      |
+----------+---------------------+---------------------+
| CLARK    | 2916.6666666666665  | 2758.3333333333335  |
| KING     | 2916.6666666666665  | 5000.0              |
| MILLER   | 2916.6666666666665  | 1037.5              |
| SMITH    | 2175.0              | 1037.5              |
| JONES    | 2175.0              | 2758.3333333333335  |
| SCOTT    | 2175.0              | 3000.0              |
| ADAMS    | 2175.0              | 1037.5              |
| FORD     | 2175.0              | 3000.0              |
| ALLEN    | 1566.6666666666667  | 1400.0              |
| WARD     | 1566.6666666666667  | 1400.0              |
| MARTIN   | 1566.6666666666667  | 1400.0              |
| BLAKE    | 1566.6666666666667  | 2758.3333333333335  |
| TURNER   | 1566.6666666666667  | 1400.0              |
| JAMES    | 1566.6666666666667  | 1037.5              |
+----------+---------------------+---------------------+


4. Divide emps by category -- Poor < 1500, 1500 <= Middle <= 2500, Rich > 2500. Hint: CASE ... WHEN. Count emps for each category.


with cte as 
(select empno,ename,case when sal<1500 then 'poor' when sal>2500 then 'rich' else 'middle' end as category from emp_staging) 
select category,count(empno) from cte group by category;


+-----------+------+
| category  | _c1  |
+-----------+------+
| middle    | 3    |
| poor      | 6    |
| rich      | 5    |
+-----------+------+


5. Display emps with category (as above), empno, ename, sal and dname.

with cte as (select empno,deptno,ename,sal,case when sal<1500 then 'poor' when sal>2500 then 'rich' else 'middle' end as category from emp_staging) select c.empno,c.ename,c.sal,d.dname from cte c inner join dept_staging d on c.deptno=d.deptno;

+----------+----------+---------+-------------+
| c.empno  | c.ename  |  c.sal  |   d.dname   |
+----------+----------+---------+-------------+
| 7369     | SMITH    | 800.0   | RESEARCH    |
| 7499     | ALLEN    | 1600.0  | SALES       |
| 7521     | WARD     | 1250.0  | SALES       |
| 7566     | JONES    | 2975.0  | RESEARCH    |
| 7654     | MARTIN   | 1250.0  | SALES       |
| 7698     | BLAKE    | 2850.0  | SALES       |
| 7782     | CLARK    | 2450.0  | ACCOUNTING  |
| 7788     | SCOTT    | 3000.0  | RESEARCH    |
| 7839     | KING     | 5000.0  | ACCOUNTING  |
| 7844     | TURNER   | 1500.0  | SALES       |
| 7876     | ADAMS    | 1100.0  | RESEARCH    |
| 7900     | JAMES    | 950.0   | SALES       |
| 7902     | FORD     | 3000.0  | RESEARCH    |
| 7934     | MILLER   | 1300.0  | ACCOUNTING  |
+----------+----------+---------+-------------+


6. Count number of emps in each dept for each category (as above).

with cte as (select deptno,empno,ename,case when sal<1500 then 'poor' when
sal>2500 then 'rich' else 'middle' end as category from emp_staging) select deptno,category,count(empno) from cte group by deptno,category order by deptno;

+---------+-----------+------+
| deptno  | category  | _c2  |
+---------+-----------+------+
| 10      | rich      | 1    |
| 10      | poor      | 1    |
| 10      | middle    | 1    |
| 20      | rich      | 3    |
| 20      | poor      | 2    |
| 30      | rich      | 1    |
| 30      | poor      | 3    |
| 30      | middle    | 2    |
+---------+-----------+------+


with cte as (select deptno,empno,ename,case when sal<1500 then 'poor' when
sal>2500 then 'rich' else 'middle' end as category from emp_staging) select c.deptno,c.category,d.dname,count(c.empno) from cte c inner join dept_staging d on c.deptno=d.deptno group by c.deptno,c.category,d.dname order by c.deptno;


+-----------+-------------+-------------+------+
| c.deptno  | c.category  |   d.dname   | _c3  |
+-----------+-------------+-------------+------+
| 10        | rich        | ACCOUNTING  | 1    |
| 10        | poor        | ACCOUNTING  | 1    |
| 10        | middle      | ACCOUNTING  | 1    |
| 20        | rich        | RESEARCH    | 3    |
| 20        | poor        | RESEARCH    | 2    |
| 30        | rich        | SALES       | 1    |
| 30        | poor        | SALES       | 3    |
| 30        | middle      | SALES       | 2    |
+-----------+-------------+-------------+------+




4. Execute following queries for books.csv dataset.
1. Create table "books_staging" and load books.csv in it.

CREATE TABLE books_staging(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    'separatorChar'=',',
    'quoteChar'='"',
    'escapeChar'='\+'
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/sunbeam/BigData/data/books.csv' INTO TABLE books_staging;

select * from books_staging;


2. Create table "books_orc" as transactional table.

CREATE TABLE books_orc(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO books_orc 
select id,name,author,subject,price from books_staging;

3. Create a materialized view for summary -- Subjectwise average book price.

CREATE MATERIALIZED VIEW  book_summary as
select subject,avg(price) avg_price from books_orc group by subject;

select * from book_summary;

4. Display a report that shows subject and average price in descending order -- on materialized view.

select subject,avg_price from book_summary order by avg_price desc;

+--------------------+--------------------+
|      subject       |     avg_price      |
+--------------------+--------------------+
| C+ Programming     | 675.214            |
| Java Programming   | 519.67             |
| Operating Systems  | 447.3836666666666  |
| C Programming      | 242.20275          |
+--------------------+--------------------+


5. Create a new file newbooks.csv.

20,Atlas Shrugged,Ayn Rand,Novel,723.90
21,The Fountainhead,Ayn Rand,Novel,923.80
22,The Archer,Paulo Cohelo,Novel,623.94
23,The Alchemist,Paulo Cohelo,Novel,634.80

6. Upload the file newbooks.csv into books_staging.

LOAD DATA LOCAL INPATH '/home/sunbeam/pracBigData/newbooks.csv' INTO TABLE books_staging;

select * from books_staging;


7. Insert "new" records from books_staging into books_orc.


INSERT INTO books_orc 
select s.*
from books_staging s 
left join books_orc o
on s.id=o.id
where o.id is null;



8. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?


select * from book_summary;

+-----------------------+-------------------------+
| book_summary.subject  | book_summary.avg_price  |
+-----------------------+-------------------------+
| C Programming         | 242.20275               |
| C+ Programming        | 675.214                 |
| Java Programming      | 519.67                  |
| Operating Systems     | 447.3836666666666       |
+-----------------------+-------------------------+

No materialized view not updated



9. Rebuild the materialized view.

ALTER MATERIALIZED VIEW book_summary REBUILD;

select * from book_summary;

+-----------------------+-------------------------+
| book_summary.subject  | book_summary.avg_price  |
+-----------------------+-------------------------+
| C Programming         | 242.20275               |
| C+ Programming        | 675.214                 |
| Java Programming      | 519.67                  |
| Novel                 | 726.6099999999999       |
| Operating Systems     | 447.3836666666666       |
+-----------------------+-------------------------+



10. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?

select subject,avg_price from book_summary order by avg_price desc;

+--------------------+--------------------+
|      subject       |     avg_price      |
+--------------------+--------------------+
| Novel              | 726.6099999999999  |
| C+ Programming     | 675.214            |
| Java Programming   | 519.67             |
| Operating Systems  | 447.3836666666666  |
| C Programming      | 242.20275          |
+--------------------+--------------------+

Yes, new books visible

11. Increase price of all Java books by 10% in books_orc.

+---------------+----------------------------------+--------------------+--------------------+------------------+
| books_orc.id  |          books_orc.name          |  books_orc.author  | books_orc.subject  | books_orc.price  |
+---------------+----------------------------------+--------------------+--------------------+------------------+
| 1001          | Exploring C                      | Yashwant Kanetkar  | C Programming      | 123.456          |
| 1002          | Pointers in C                    | Yashwant Kanetkar  | C Programming      | 371.019          |
| 1003          | ANSI C Programming               | E Balaguruswami    | C Programming      | 334.215          |
| 1004          | ANSI C Programming               | Dennis Ritchie     | C Programming      | 140.121          |
| 2001          | C+ Complete Reference            | Herbert Schildt    | C+ Programming     | 417.764          |
| 2002          | C+ Primer                        | Stanley Lippman    | C+ Programming     | 620.665          |
| 2003          | C+ Programming Language          | Bjarne Stroustrup  | C+ Programming     | 987.213          |
| 3001          | Java Complete Reference          | Herbert Schildt    | Java Programming   | 525.121          |
| 3002          | Core Java Volume I               | Cay Horstmann      | Java Programming   | 575.651          |
| 3003          | Java Programming Language        | James Gosling      | Java Programming   | 458.238          |
| 4001          | Operatig System Concepts         | Peter Galvin       | Operating Systems  | 567.391          |
| 4002          | Design of UNIX Operating System  | Mauris J Bach      | Operating Systems  | 421.938          |
| 4003          | UNIX Internals                   | Uresh Vahalia      | Operating Systems  | 352.822          |
| 20            | Atlas Shrugged                   | Ayn Rand           | Novel              | 723.9            |
| 21            | The Fountainhead                 | Ayn Rand           | Novel              | 923.8            |
| 22            | The Archer                       | Paulo Cohelo       | Novel              | 623.94           |
| 23            | The Alchemist                    | Paulo Cohelo       | Novel              | 634.8            |
+---------------+----------------------------------+--------------------+--------------------+------------------+


update books_orc set price=(1.1*price);

+---------------+----------------------------------+--------------------+--------------------+---------------------+
| books_orc.id  |          books_orc.name          |  books_orc.author  | books_orc.subject  |   books_orc.price   |
+---------------+----------------------------------+--------------------+--------------------+---------------------+
| 1001          | Exploring C                      | Yashwant Kanetkar  | C Programming      | 135.8016            |
| 1002          | Pointers in C                    | Yashwant Kanetkar  | C Programming      | 408.12090000000006  |
| 1003          | ANSI C Programming               | E Balaguruswami    | C Programming      | 367.6365            |
| 1004          | ANSI C Programming               | Dennis Ritchie     | C Programming      | 154.1331            |
| 2001          | C+ Complete Reference            | Herbert Schildt    | C+ Programming     | 459.54040000000003  |
| 2002          | C+ Primer                        | Stanley Lippman    | C+ Programming     | 682.7315            |
| 2003          | C+ Programming Language          | Bjarne Stroustrup  | C+ Programming     | 1085.9343000000001  |
| 3001          | Java Complete Reference          | Herbert Schildt    | Java Programming   | 577.6331            |
| 3002          | Core Java Volume I               | Cay Horstmann      | Java Programming   | 633.2161            |
| 3003          | Java Programming Language        | James Gosling      | Java Programming   | 504.06180000000006  |
| 4001          | Operatig System Concepts         | Peter Galvin       | Operating Systems  | 624.1301            |
| 4002          | Design of UNIX Operating System  | Mauris J Bach      | Operating Systems  | 464.1318            |
| 4003          | UNIX Internals                   | Uresh Vahalia      | Operating Systems  | 388.10420000000005  |
| 20            | Atlas Shrugged                   | Ayn Rand           | Novel              | 796.2900000000001   |
| 21            | The Fountainhead                 | Ayn Rand           | Novel              | 1016.1800000000001  |
| 22            | The Archer                       | Paulo Cohelo       | Novel              | 686.3340000000001   |
| 23            | The Alchemist                    | Paulo Cohelo       | Novel              | 698.28              |
+---------------+----------------------------------+--------------------+--------------------+---------------------+



12. Rebuild the materialized view.

ALTER MATERIALIZED VIEW book_summary REBUILD;

13. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?


Before--

+--------------------+--------------------+
|      subject       |     avg_price      |
+--------------------+--------------------+
| Novel              | 726.6099999999999  |
| C+ Programming     | 675.214            |
| Java Programming   | 519.67             |
| Operating Systems  | 447.3836666666666  |
| C Programming      | 242.20275          |
+--------------------+--------------------+

After--

select subject,avg_price from book_summary order by avg_price desc;

+--------------------+---------------------+
|      subject       |      avg_price      |
+--------------------+---------------------+
| Novel              | 799.271             |
| C+ Programming     | 742.7354            |
| Java Programming   | 571.6370000000001   |
| Operating Systems  | 492.1220333333333   |
| C Programming      | 266.42302500000005  |
+--------------------+---------------------+


14. Delete all Java books.

delete from books_orc where subject="Java Programming";

select * from books_orc


15. Rebuild the materialized view.

ALTER MATERIALIZED VIEW book_summary REBUILD;

16. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?

select subject,avg_price from book_summary order by avg_price desc;

BEFORE--
+--------------------+---------------------+
|      subject       |      avg_price      |
+--------------------+---------------------+
| Novel              | 799.271             |
| C+ Programming     | 742.7354            |
| Java Programming   | 571.6370000000001   |
| Operating Systems  | 492.1220333333333   |
| C Programming      | 266.42302500000005  |
+--------------------+---------------------+

AFTER--

+--------------------+---------------------+
|      subject       |      avg_price      |
+--------------------+---------------------+
| Novel              | 799.271             |
| C+ Programming     | 742.7354            |
| Operating Systems  | 492.1220333333333   |
| C Programming      | 266.42302500000005  |
+--------------------+---------------------+



5. Execute following queries for movies dataset.

1. Upload movies data (movies_caret.csv) into HDFS directory (not in hive warehouse).

terminal-
hadoop fs -mkdir -p /user/$USER/assignment/input

hadoop fs -put /home/aditi/pracBigData/movies/movies_caret.csv /user/$USER/assignment/input

hadoop fs -cat /user/$USER/assignment/input/movies_caret.csv



2. Create external table movies1 with schema - id INT, title STRING, genres STRING.
Find number of 'Action' movies.

CREATE EXTERNAL TABLE movies1(
    id INT,
    title STRING,
    genres STRING
)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    'separatorChar'='^',
    'quoteChar'='"',
    'escapeChar'='\\|'
)
STORED AS TEXTFILE
LOCATION '/user/sunbeam/assignment/input'
TBLPROPERTIES('skip.header.line.count'='1')
;






3. Create external table movies2 with schema - id INT, title STRING, genres ARRAY<STRING>.
Find number of movies having single genre.

CREATE EXTERNAL TABLE movies2(
    id INT,
    title STRING,
    genres ARRAY<STRING>
)
STORED AS ORC
TBLPROPERTIES('skip.header.line.count'='1');

insert into movies2
select id,title,split(genres,'\\|') from movies1;


SELECT count(id) from movies2 where ARRAY_CONTAINS(genres,'Action');

+-------+
|  _c0  |
+-------+
| 1545  |
+-------+


SELECT count(id) from movies2 where SIZE(genres)=1;

+-------+
|  _c0  |
+-------+
| 2793  |
+-------+


6. Upload busstops.json data into HDFS directory. Then create hive external table to fetch data using JsonSerDe.



hadoop fs -mkdir -p /user/$USER/busstops/input

hadoop fs -put /home/aditi/BigData/data/bus.json /user/aditi/busstops/input


CREATE EXTERNAL TABLE bus1(
    `_id` STRUCT<`$oid`:STRING>,
    stop STRING,
    code STRING,
    seq DOUBLE,
    stage DOUBLE,
    name STRING,
    location STRUCT<type:STRING, coordinates:ARRAY<DOUBLE>>
)
ROW FORMAT
SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/sunbeam/busstops/input'
;


/home/aditi/.m2/repository/org/apache/hive/hive-jdbc/3.1.3/
