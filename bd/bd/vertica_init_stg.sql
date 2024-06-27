CREATE TABLE chats
(
    msg_id       INT PRIMARY KEY,
    msg_time     TIMESTAMP,
    msg_from     INT,
    msg_to       INT,
    text_message VARCHAR(255),
    msg_group_id INT
);

CREATE TABLE groups
(
    id           INT PRIMARY KEY,
    owner_id     INT,
    group_name   VARCHAR(255),
    created_date TIMESTAMP
);

CREATE TABLE peoples
(
    id                INT PRIMARY KEY,
    name              VARCHAR(255),
    registration_date TIMESTAMP,
    country           VARCHAR(255),
    date_of_birthday  DATE,
    phone             VARCHAR(20),
    e_mail            VARCHAR(255)
);



-- топ 5 групп по кол сообщений
CREATE OR REPLACE VIEW top_5_groups AS
SELECT DISTINCT msg_group_id,
                count(chats.msg_from) as msg
FROM public.chats
GROUP BY msg_group_id
ORDER BY msg DESC
LIMIT 5;

-- находим людей которые писали в эти группы
CREATE OR REPLACE VIEW peoples_who_wrote_in_top AS
select msg_from from chats
    where chats.msg_group_id IN(select msg_group_id
                    from top_5_groups);

