CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT not null,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid,
    film_work_id uuid,
    created timestamp with time zone,

    FOREIGN KEY (genre_id) REFERENCES content.genre (id) ON DELETE CASCADE,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    role TEXT,
    created timestamp with time zone,

    FOREIGN KEY (person_id) REFERENCES content.person (id) ON DELETE CASCADE,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work (id) ON DELETE CASCADE
);

-- Один актер учитывается в одном фильме ровно один раз.
CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_idx ON content.person_film_work (film_work_id, person_id, role);

CREATE UNIQUE INDEX IF NOT EXISTS genre_film_work_idx ON content.genre_film_work (film_work_id, genre_id);

-- Имя актера уникальное.
CREATE UNIQUE INDEX IF NOT EXISTS full_name_person_idx ON content.person (full_name);

-- Имя жанра уникальное.
CREATE UNIQUE INDEX IF NOT EXISTS name_genre_idx ON content.genre (name);


CREATE OR REPLACE FUNCTION insert_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    NEW.created = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


DROP TRIGGER IF EXISTS insert_genre_modtime on content.genre;
CREATE TRIGGER insert_genre_modtime BEFORE INSERT ON content.genre FOR EACH ROW EXECUTE PROCEDURE insert_modified_column();

DROP TRIGGER IF EXISTS insert_person_modtime on content.person;
CREATE TRIGGER insert_person_modtime BEFORE INSERT ON content.person FOR EACH ROW EXECUTE PROCEDURE  insert_modified_column();

DROP TRIGGER IF EXISTS insert_film_work_modtime on content.film_work;
CREATE TRIGGER insert_film_work_modtime BEFORE INSERT ON content.film_work FOR EACH ROW EXECUTE PROCEDURE  insert_modified_column();

DROP TRIGGER IF EXISTS update_genre_modtime on content.genre;
CREATE TRIGGER update_genre_modtime BEFORE UPDATE ON content.genre FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();


DROP TRIGGER IF EXISTS update_person_modtime on content.person;
CREATE TRIGGER update_person_modtime BEFORE UPDATE ON content.person FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();

DROP TRIGGER IF EXISTS update_film_work_modtime on content.film_work;
CREATE TRIGGER update_film_work_modtime BEFORE UPDATE ON content.film_work FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();