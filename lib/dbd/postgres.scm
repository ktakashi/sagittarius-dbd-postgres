;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; dbd/postgresql.scm - DBD for PostgreSQL
;;;  
;;;   Copyright (c) 2015  Takashi Kato  <ktakashi@ymail.com>
;;;   
;;;   Redistribution and use in source and binary forms, with or without
;;;   modification, are permitted provided that the following conditions
;;;   are met:
;;;   
;;;   1. Redistributions of source code must retain the above copyright
;;;      notice, this list of conditions and the following disclaimer.
;;;  
;;;   2. Redistributions in binary form must reproduce the above copyright
;;;      notice, this list of conditions and the following disclaimer in the
;;;      documentation and/or other materials provided with the distribution.
;;;  
;;;   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;;;   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;;;   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;;;   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;;;   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;;;   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
;;;   TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
;;;   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
;;;   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;;   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;;   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;  

(library (dbd postgres)
    (export make-postgres-driver)
    (import (rnrs)
	    (rnrs mutable-pairs)
	    (postgresql)
	    (dbi)
	    (clos user)
	    (util list) ;; for map-with-index
	    (sagittarius)
	    (sagittarius control)
	    (sagittarius object))

;; driver
(define-class <dbi-postgres-driver> (<dbi-driver>) ())

(define-class <state-mixin> ()
  ((open? :init-value #t)))
(define-class <dbi-postgres-connection> (<dbi-connection> <state-mixin>)
  ((connection :init-keyword :connection :reader postgres-connection)
   (auto-commit :init-keyword :auto-commit)))
(define-class <dbi-postgres-query> (<dbi-query> <state-mixin>)
  ;; temporary storage to keep the result of dbi-execute!
  ((parameters :init-value '() :init-keyword :parameters)
   (query :init-value #f :reader postgres-query-query)
   (end? :init-value #f)))

;; TODO
(define-class <dbi-postgres-table> (<dbi-table>)   ())
(define-class <dbi-postgres-column> (<dbi-column>) ())

(define (emit-begin con) (postgresql-execute-sql! con "BEGIN"))

(define-method dbi-make-connection ((driver <dbi-postgres-driver>)
				    options option-alist 
				    :key (username "") (password "")
				    (auto-commit #f)
				    :allow-other-keys)
    (define (get-option name :optional (default #f))
      (or (and-let* ((v (assoc name option-alist))) (cdr v))
	  default))
    (define (->boolean s)
      (if (string? s) (not (string=? s "false")) s))
    (define (make-dsn name host port proto sid?)
      (construct-connection-string name host :port port :protocol proto
				   :use-sid sid?))
    (let ((database (get-option "database"))
	  (host     (get-option "host"))
	  (port     (get-option "port" "5432")))
      (unless host
	(assertion-violation 'dbi-make-connection 
			     "host option is required"))
      (let ((conn (make-postgresql-connection host port 
					      database username password)))
	;; open it
	(postgresql-open-connection! conn)
	;; make it secure if the server supports it.
	(postgresql-secure-connection! conn)
	;; login
	(postgresql-login! conn)
	;; manually do those thing...
	(unless auto-commit (emit-begin conn))
	(make <dbi-postgres-connection> :connection conn
	      :auto-commit auto-commit))))

(define-method dbi-open? ((conn <dbi-postgres-connection>))
  (and (postgres-connection conn) #t))

(define-method dbi-close ((conn <dbi-postgres-connection>))
  (let1 con (postgres-connection conn)
    ;; must always be rollback
    (unless (~ conn 'auto-commit) (postgresql-rollback! con))
    (postgresql-terminate! con))
  (set! (~ conn 'connection) #f))

;; convert ? to $1 $2 ...
(define (convert-sql sql)
  (let-values (((out extract) (open-string-output-port)))
    (let1 counter 0
      (string-for-each (lambda (c)
			 (cond ((char=? c #\?)
				(set! counter (+ counter 1))
				(display #\$ out)
				(display counter out))
			       (else (display c out)))) sql))
    (extract)))

(define-method dbi-prepare ((conn <dbi-postgres-connection>)
			    sql . args)
  (let1 stmt (postgresql-prepared-statement (postgres-connection conn) 
					    (convert-sql sql))
    (make <dbi-postgres-query> 
      :prepared stmt 
      :connection conn
      :parameters (map-with-index (lambda (i v) (cons (+ i 1) v)) args))))

(define-method dbi-open? ((q <dbi-postgres-query>))
  (and (~ q 'prepared) #t))

(define-method dbi-close ((q <dbi-postgres-query>))
  (when (~ q 'prepared)
    (postgresql-close-prepared-statement! (~ q 'prepared)))
  (set! (~ q 'prepared) #f))

(define-method dbi-commit! ((conn <dbi-postgres-connection>))
  (let1 r (postgresql-commit! (postgres-connection conn))
    (unless (~ conn 'auto-commit) (emit-begin (postgres-connection conn)))
    r))

(define-method dbi-rollback! ((conn <dbi-postgres-connection>))
  (let1 r (postgresql-rollback! (postgres-connection conn))
    (unless (~ conn 'auto-commit) (emit-begin (postgres-connection conn)))
    r))

;; we need to send parameter when it's executed otherwise doesn't work
(define-method dbi-bind-parameter! ((query <dbi-postgres-query>)
				    (index <integer>) value)
  (let ((params (~ query 'parameters)))
    (cond ((assv index params) => (lambda (s) (set-cdr! s value)))
	  (else (set! (~ query 'parameters) (acons index value params))))))

(define-method dbi-execute! ((query <dbi-postgres-query>) . args)
  (let ((stmt (dbi-query-prepared query)))
    (unless (null? args)
      ;; bind
      (do ((i 1 (+ i 1)) (params args (cdr params)))
	  ((null? params) #t)
	(dbi-bind-parameter! query i (car params))))
    (apply postgresql-bind-parameters! stmt
	   (map cdr (list-sort (lambda (a b) (< (car a) (car b)))
			       (~ query 'parameters))))
    ;; reset end flag
    (set! (~ query 'end?) #f)
    (let1 q (postgresql-execute! stmt)
      (if (postgresql-query? q)
	  (begin (set! (~ query 'query) q) -1)
	  ;; modification?
	  q))))

(define-method dbi-fetch! ((query <dbi-postgres-query>))
  (if (~ query 'end?)
      #f
      (let1 q (~ query 'query)
	(if q
	    (let ((r (postgresql-fetch-query! q)))
	      (unless r (set! (~ query 'end?) #t))
	      r)
	    (error 'dbi-fetch! "query is not SELECT")))))

(define-method dbi-commit! ((query <dbi-postgres-query>))
  (let ((con (dbi-query-connection query)))
    (dbi-commit! con)))

(define-method dbi-rollback! ((query <dbi-postgres-query>))
  (let ((con (dbi-query-connection query)))
    (dbi-rollback! con)))

(define-method dbi-columns ((query <dbi-postgres-query>))
  (vector-map (lambda (v) (vector-ref v 0))
	      (postgresql-query-descriptions (postgres-query-query query))))

(define (make-postgres-driver) (make <dbi-postgres-driver>))

;; TODO 
(define-method dbi-tables ((conn <dbi-postgres-connection>)
			   :key (schema "") (table "") (types '(table view)))
  (error 'dbi-tables "not supported" conn))

;; TODO 
(define-method dbi-table-columns ((table <dbi-postgres-table>))
  (error 'dbi-tables "not supported" table))


)
