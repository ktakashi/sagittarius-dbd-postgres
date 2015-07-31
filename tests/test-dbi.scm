(import (rnrs)
	(dbi)
	(clos user)
	(postgresql)
	(srfi :64))

(let ((conn (make-postgresql-connection "localhost" "5432"
					#f "postgres" "postgres")))
  (postgresql-open-connection! conn)
  (postgresql-login! conn)
  (guard (e (else #t)) (postgresql-execute-sql! conn "drop table test")))

(test-begin "DBD PostgreSQL")

(let ((conn (dbi-connect "dbi:postgres:host=localhost;"
			 :username "postgres" :password "postgres")))
  (test-assert "connection?" (is-a? conn <dbi-connection>))
  (test-assert "create table"
	       (dbi-execute-using-connection! 
		conn "create table test (id integer, name varchar(255))"))
  ;; Seems, if we put 'BEGIN' on PostgreSQL, we can even rollback DDL.
  ;; i'm kinda surprised!.
  (dbi-commit! conn)
  (test-assert "disconnect" (dbi-close conn)))

;; do some transaction
(define conn (dbi-connect "dbi:postgres:host=localhost;"
			  :username "postgres" :password "postgres"))

(let ((p (dbi-prepare conn "insert into test (id, name) values (?, ?)")))
  (test-assert "bind (1)" (dbi-bind-parameter! p 1 1))
  (test-assert "bind (2)" (dbi-bind-parameter! p 2 "name"))
  (test-equal "execute" 1 (dbi-execute! p))
  (test-assert "commit" (dbi-commit! p))
  (test-assert "close" (dbi-close p)))


(let ((p (dbi-prepare conn "insert into test (id, name) values (?, ?)")))
  (test-assert "bind (3)" (dbi-bind-parameter! p 1 2))
  (test-assert "bind (4)" (dbi-bind-parameter! p 2 "name2"))
  (test-equal "execute" 1 (dbi-execute! p))
  (test-assert "rollback" (dbi-rollback! p))
  ;; reuse
  (test-equal "execute" 1 (dbi-execute! p 3 "name3"))
  (test-assert "commit" (dbi-commit! p))
  (test-assert "close" (dbi-close p)))

(let ((p (dbi-prepare conn "select count(*) from test")))
  (test-equal "execute" -1 (dbi-execute! p))
  (test-equal "count" #(2) (dbi-fetch! p))
  (test-assert "close" (dbi-close p)))

(test-assert "drop" (dbi-execute-using-connection! conn "drop table test"))
(dbi-close conn)

(test-end)
