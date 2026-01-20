-- Recup les infos d'un etudiant 

select * from student where id = 1;

--recup toutes les inscriptions d'un etudiant

select * from enrollments where student_id = 1;

-- Recup tous les accès a un cours sur une periode de 7 jours 

select * from access_log
where course_id = 10
and access_date between '2026-01-10' and '2026-01-17';

-- Nombre d'isncription par cours 

select course_id, count(*) as nombre_inscriptions
from enrollments
group by course_id;
order by nombre_inscriptions desc;

-- Nombre d'accès par cours 

select course_id, count(*) as nombre_accès
from access_log
group by course_id;
order by nombre_accès desc;


-- les cours sur lequel un étudiant est inscrit

select courses.* from enrollments
join courses on courses.id = enrollments.course_id
where enrollments.student_id = 103;

-- les étudiant qui ont au moins 10 inscriptions 

select student_id, count(*) as nombre_inscriptions from enrollments
group by student_id
having count(*) >= 10;
order by nombre_inscriptions desc;

-- nombre d'accès par jour sur un cours donné

select date(access_date) as jour, count(*) as nombre_accès 
from access_log
where course_id = 100 
group by date(access_date)
order by jour desc;

-- les 50 étudiant les plus actif 

select student_id, count(*) from access_log
group by student_id
order by count(*) desc


-- les 10 inscription avec détail d'un étudaint + cours 

select student.id, student.name, courses.name, enrollments.enrollment_date from enrollments
join student on student.id = enrollments.student_id
join courses on courses.id = enrollments.course_id
where enrollments.enrollment_date > now() - interval '30 days'
order by enrollments.enrollment_date desc

