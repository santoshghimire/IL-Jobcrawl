DROP TABLE IF EXISTS  sites_datas;

CREATE TABLE sites_datas(

            Site VARCHAR (255),
            Company VARCHAR (255),
            Company_jobs VARCHAR (255),
            Job_id INTEGER(10),
            Job_title VARCHAR (255),
            Job_Description TEXT ,
            Job_Post_Date VARCHAR (20),
            Job_URL TEXT,
            Country_Areas VARCHAR (255),
            Job_categories VARCHAR (255),
            AllJobs_Job_class VARCHAR (255),
            Crawl_Date VARCHAR (20),
            unique_id VARCHAR (255),
            PRIMARY KEY (Crawl_Date,unique_id)

) DEFAULT CHARSET = utf8;