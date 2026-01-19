create table "student" (
    "id" serial primary key,
    "name" varchar(50) not null,
    "age" integer not null,
    "email" varchar(155) not null,
    "phone" varchar(10) not null,
    "address" varchar(100) not null,
    "city" varchar(50) not null,
    "state" varchar(50) not null,
    "zip" integer not null,
    "country" varchar(50) not null,
    "created_at" timestamp not null default current_timestamp,
    "updated_at" timestamp not null default current_timestamp
);

create table "courses" (
    "id" serial primary key,
    "name" varchar(50) not null,
    "description" varchar(155) not null,
    "created_at" timestamp not null default current_timestamp,
    "updated_at" timestamp not null default current_timestamp
);

create table "enrollments" (
    "id" serial primary key,
    "student_id" integer not null,
    "course_id" integer not null,
    "enrollment_date" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    "updated_at" timestamp not null default current_timestamp,
    constraint "enrollments_student_id_fkey" foreign key ("student_id") references "student" ("id") on delete cascade,
    constraint "enrollments_course_id_fkey" foreign key ("course_id") references "courses" ("id") on delete cascade
);

create table "access_log" (
    "id" serial primary key,
    "student_id" integer not null,
    "course_id" integer not null,
    "access_date" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    "updated_at" timestamp not null default current_timestamp,
    constraint "access_log_student_id_fkey" foreign key ("student_id") references "student" ("id") on delete cascade,
    constraint "access_log_course_id_fkey" foreign key ("course_id") references "courses" ("id") on delete cascade
);

create index "idx_enrollments_student_id" on "enrollments" ("student_id");
create index "idx_enrollments_course_id" on "enrollments" ("course_id");

create index "idx_access_log_student_id" on "access_log" ("student_id");
create index "idx_access_log_course_id" on "access_log" ("course_id");
create index "idx_access_log_access_date" on "access_log" ("access_date");
