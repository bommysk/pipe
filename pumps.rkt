;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Ray Racine's TR Library
;; Copyright (C) 2007-2013  Raymond Paul Racine
;;
;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#lang typed/racket/base

(provide:
 [pump/eos    (All (D A) (Pump D A))]
 [pump/list   (All (D A) (Listof D) -> (Pump D A))]
 [pump/vector (All (D A) (Vectorof D) -> (Pump D A))]
 [pump/text-input-port     (All (A) (Input-Port -> (Pump String A)))]
 [pump/select-from-n-lists (All (D A) (Listof (Listof D)) (D D -> Boolean) -> (Pump D A))])

(require
 racket/match
 (only-in racket/unsafe/ops
	  unsafe-vector-ref)
 (only-in "types.rkt"
	  Done Continue Tank Pump))

(: pump/eos (All (D A) (Pump D A)))
(define (pump/eos iter)
  (match iter
	 [(Done _ _) iter]
	 [(Continue step) (step 'EOS)]))

;; Useful for testing, i.e. simulate an IO read of a string
(: pump/string (All (A) String -> (Pump String A)))
(define (pump/string str)
  (λ: ((iter : (Tank String A)))
      (match iter
	     [(Done _ _) iter]
	     [(Continue k) (k str)])))

(: pump/list (All (D A) (Listof D) -> (Pump D A)))
(define (pump/list lst)
  (λ: ((iter : (Tank D A)))
      (let: loop : (Tank D A) ((lst : (Listof D) lst)
			       (iter : (Tank D A) iter))
	    (match (cons lst iter)
		   [(cons '() i)           i]
		   [(cons _ (Done _ _))    iter]
		   [(cons (list-rest x xs) (Continue k)) (loop xs (k x))]))))

(: pump/vector (All (D A) (Vectorof D) -> (Pump D A)))
(define (pump/vector vect)
  (define: len : Index (vector-length vect))
  (λ: ((iter : (Tank D A)))
      (let: loop : (Tank D A)
	    ((idx : Natural 0)
	     (iter : (Tank D A) iter))
	    (if (>= idx len)
		iter
		(match iter
		       [(Done _ _) iter]
		       [(Continue k) (loop (add1 idx)
					   (k (unsafe-vector-ref vect idx)))])))))

(: pump/text-input-port (All (A) (Input-Port -> (Pump String A))))
(define (pump/text-input-port inp)
  (λ: ((iter : (Tank String A)))
      (let loop ((iter iter))
	(match iter
	       [(Done _ _) iter]
	       [(Continue step)
		(let ((line (read-line inp)))
		  (if (eof-object? line)
		      iter
		      (loop (step line))))]))))

;; Given multiple lists select the next element from one of the lists using the selector until all lists are exhausted.
;; selector - when true the first arg is selected else the second
;; e.g, if selector is a less-than? comparison function the enumerator feeds the least head item from the lists.
;;      and if the lists themselves were sorted, the enumerator feed is sorted.
(: pump/select-from-n-lists (All (D A) (Listof (Listof D)) (D D -> Boolean) -> (Pump D A)))
(define (pump/select-from-n-lists data-lsts select-first?)

  (define: lsts : (Vectorof (Listof D)) (list->vector data-lsts))
  (define: lsts-cnt : Index (vector-length lsts))

  (: head (Natural -> (Option D)))
  (define (head idx)
    (let ((from-lst (vector-ref lsts idx)))
      (if (null? from-lst)
	  #f
	  (car from-lst))))

  (: pop (Natural -> (Option D)))
  (define (pop idx)
    (let ((datum (head idx)))
      (when datum
	    (vector-set! lsts idx (cdr (vector-ref lsts idx))))
      datum))

  (: next-selected (-> (Option D)))
  (define (next-selected)
    (let: loop : (Option D) ((idx : Natural 0)
			     (widx : Natural 0)
			     (winner : (Option D) (head 0)))
	  (if (>= idx lsts-cnt)
	      (pop widx)
	      (let: ((next : (Option D) (head idx)))
		    (if next
			(if winner
			    (if (select-first? winner next)
				(loop (add1 idx) widx winner)
				(loop (add1 idx) idx next))
			    (loop (add1 idx) idx next))
			(loop (add1 idx) widx winner))))))

  (λ: ((iter : (Tank D A)))
      (let: loop : (Tank D A) ((iter : (Tank D A) iter))
	    (match iter
		   [(and done (Done _ _)) done]
		   [(Continue k)
		    (let ((selected (next-selected)))
		      (if selected
			  (loop (k selected))
			  (k 'EOS)))]))))
