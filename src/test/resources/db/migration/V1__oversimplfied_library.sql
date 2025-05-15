CREATE TABLE "author" (
    "name" VARCHAR(256) NOT NULL PRIMARY KEY,
    "birth_date" DATE
);

CREATE TABLE "book"(
    "title" VARCHAR(128) NOT NULL PRIMARY KEY,
    "author" VARCHAR(256) REFERENCES "author"("name")
);

INSERT INTO "author"("name", "birth_date") VALUES ('Philip K. Dick', '1928-12-16');
INSERT INTO "book"("title", "author") VALUES ('Do Androids Dream of Electric Sheep?', 'Philip K. Dick');
INSERT INTO "book"("title", "author") VALUES ('Second variety', 'Philip K. Dick');
INSERT INTO "book"("title", "author") VALUES ('Ubik', 'Philip K. Dick');

INSERT INTO "author"("name", "birth_date") VALUES ('Arthur C. Clark', '1917-12-16');
INSERT INTO "book"("title", "author") VALUES ('Rendezvous with Rama', 'Arthur C. Clark');
INSERT INTO "book"("title", "author") VALUES ('2001: A Space Odyssey', 'Arthur C. Clark');
