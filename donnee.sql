truncate table "access_log", "enrollments", "courses", "student" restart identity cascade;

insert into "student" (
  "name",
  "age",
  "email",
  "phone",
  "address",
  "city",
  "state",
  "zip",
  "country",
  "created_at",
  "updated_at"
)
select
  'Student ' || g as "name",
  (18 + floor(random() * 50))::integer as "age",
  'student' || g || '@example.com' as "email",
  lpad((floor(random() * 10000000000))::bigint::text, 10, '0') as "phone",
  'Address ' || g as "address",
  'City ' || (1 + (g % 100)) as "city",
  'State ' || (1 + (g % 50)) as "state",
  (10000 + (g % 90000))::integer as "zip",
  'Country ' || (1 + (g % 10)) as "country",
  now() as "created_at",
  now() as "updated_at"
from generate_series(1, 200000) as g;

insert into "courses" (
  "name",
  "description",
  "created_at",
  "updated_at"
)
select
  'Course ' || g as "name",
  'Description for course ' || g as "description",
  now() as "created_at",
  now() as "updated_at"
from generate_series(1, 1000) as g;

insert into "enrollments" (
  "student_id",
  "course_id",
  "enrollment_date",
  "created_at",
  "updated_at"
)
select
  (floor(random() * 200000)::integer + 1) as "student_id",
  (floor(random() * 1000)::integer + 1) as "course_id",
  now() - (floor(random() * 365)::integer || ' days')::interval as "enrollment_date",
  now() as "created_at",
  now() as "updated_at"
from generate_series(1, 2000000) as g;


insert into "access_log" (
  "student_id",
  "course_id",
  "access_date",
  "created_at",
  "updated_at"
)
select
  (floor(random() * 200000)::integer + 1) as "student_id",
  (floor(random() * 1000)::integer + 1) as "course_id",
  now() - (floor(random() * 365)::integer || ' days')::interval as "access_date",
  now() as "created_at",
  now() as "updated_at"
from generate_series(1, 5000000) as g;