-- Создание таблиц с соблюдением PK и FK

-- Таблица h_people
CREATE TABLE h_people
(
    hk_person_id VARCHAR(64) PRIMARY KEY ENABLED,
    person_id    VARCHAR(64),
    source       VARCHAR(255),
    load_date    TIMESTAMP default now()
);

-- Таблица s_person_info
CREATE TABLE s_person_info
(
    hk_person_id     VARCHAR(64) PRIMARY KEY ENABLED,
    name             VARCHAR(255),
    date_of_birthday DATE,
    country          VARCHAR(255),
    phone            VARCHAR(20),
    email            VARCHAR(255),
    source           VARCHAR(255),
    load_date        TIMESTAMP default now(),
    FOREIGN KEY (hk_person_id) REFERENCES h_people (hk_person_id)
);

-- Таблица h_groups
CREATE TABLE h_groups
(
    hk_group_id  VARCHAR(64) PRIMARY KEY ENABLED,
    group_id     VARCHAR(64),
    created_date TIMESTAMP,
    source       VARCHAR(255),
    load_date    TIMESTAMP default now()
);

-- Таблица s_group_info
CREATE TABLE s_group_info
(
    hk_group_id VARCHAR(64) PRIMARY KEY ENABLED,
    title       VARCHAR(255),
    status      VARCHAR(50),
    source      VARCHAR(255),
    load_date   TIMESTAMP default now(),
    FOREIGN KEY (hk_group_id) REFERENCES h_groups (hk_group_id)
);

-- Таблица h_chats
CREATE TABLE h_chats
(
    hk_msg_id VARCHAR(64) PRIMARY KEY ENABLED,
    msg_id    VARCHAR(64),
    msg_time  TIMESTAMP,
    source    VARCHAR(255),
    load_date TIMESTAMP default now()
);

-- Таблица s_chat_info
CREATE TABLE s_chat_info
(
    hk_msg_id VARCHAR(64) PRIMARY KEY ENABLED,
    text_msg  VARCHAR(2000),
    msg_from  VARCHAR(64),
    msg_to    VARCHAR(64),
    status    VARCHAR(50),
    source    VARCHAR(255),
    load_date TIMESTAMP default now(),
    FOREIGN KEY (hk_msg_id) REFERENCES h_chats (hk_msg_id)
);

-- Таблица L_chat_owners
CREATE TABLE L_chat_owners
(
    hk_L_owner_id VARCHAR(64) PRIMARY KEY ENABLED,
    hk_person_id  VARCHAR(64),
    hk_group_id   VARCHAR(64),
    source        VARCHAR(255),
    load_date     TIMESTAMP default now(),
    FOREIGN KEY (hk_person_id) REFERENCES h_people (hk_person_id),
    FOREIGN KEY (hk_group_id) REFERENCES h_groups (hk_group_id)
);

-- Таблица L_person_chat
CREATE TABLE L_person_chat
(
    hk_L_person_chat VARCHAR(64) PRIMARY KEY ENABLED,
    hk_person_id     VARCHAR(64),
    hk_msg_id        VARCHAR(64),
    source           VARCHAR(255),
    load_date        TIMESTAMP default now(),
    FOREIGN KEY (hk_person_id) REFERENCES h_people (hk_person_id),
    FOREIGN KEY (hk_msg_id) REFERENCES h_chats (hk_msg_id)
);

-- Таблица L_groups
CREATE TABLE L_groups
(
    hk_L_group_id VARCHAR(64) PRIMARY KEY ENABLED,
    hk_group_id   VARCHAR(64),
    hk_msg_id     VARCHAR(64),
    source        VARCHAR(255),
    load_date     TIMESTAMP default now(),
    FOREIGN KEY (hk_group_id) REFERENCES h_groups (hk_group_id),
    FOREIGN KEY (hk_msg_id) REFERENCES h_chats (hk_msg_id)
);
