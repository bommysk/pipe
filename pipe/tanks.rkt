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
 [show         (All (D) (Output-Port -> (Tank D Void)))]
 [print        (All (D) (Output-Port -> (Tank D Void)))]
 [head         (All (D) (-> (Tank D (Option D))))]
 [head-n       (All (D) (Integer -> (Tank D (Listof D))))]
 [drop         (All (D) Integer -> (Tank D Void))]
 [counter      (All (D) (-> (Tank D Integer)))]
 [sum          (-> (Tank Number Number))]
 [sum-r        (-> (Tank Real Real))]
 [sum-i        (-> (Tank Integer Integer))]
 [sum1-i       (-> (Tank Integer Integer))] 
 [dev/null     (All (D) (-> (Tank D Void)))]
 [and-iteratee (All (D) ((D -> Boolean) -> (Tank D Boolean)))]
 [set-tank     (All (D) (-> (Tank D (Setof D))))]
 [list-tank    (All (D) (-> (Tank D (Listof D))))]
 [vector-tank  (All (D) ((Vectorof D) -> (Tank D (Vectorof D))))]
 [hash-tank    (All (K V) (-> (Tank (Pair K V) (HashTable K V))))])

(require  
 racket/match
 (only-in racket/set
	  set set-add)
 (only-in "types.rkt"
	  Tank Stream Continue Done))

(: counter (All (D) (-> (Tank D Integer))))
(define (counter)
  (: step (Integer -> ((Stream D) -> (Tank D Integer))))
  (define (step n)
    (λ: ((str : (Stream D)))
	(match str
	       ['Nothing  (Continue (step n))]
	       ['EOS      (Done 'EOS n)]
	       [_         (Continue (step (add1 n)))])))
  (Continue (step 0)))

(: drop (All (D) Integer -> (Tank D Void)))
(define (drop n)
  (: step (-> ((Stream D) -> (Tank D Void))))
  (define (step) 
    (λ: ((str : (Stream D)))
	(match str
	       ['Nothing  (Continue (step))]
	       ['EOS      (Done 'EOS (void))]
	       [_         (drop (sub1 n))])))
  (if (zero? n)
      (Done 'Nothing (void))
      (Continue (step))))

(: head (All (D) (-> (Tank D (Option D)))))
(define (head)
  (: step ((Stream D) -> (Tank D (Option D))))
  (define step
    (λ (str)
      (cond
       [(eq? str 'Nothing)  (Continue step)]
       [(eq? str 'EOS)      (Done 'EOS #f)]
       [else                 (Done 'EOS str)])))
  (Continue step))

(: head-n (All (D) (Integer -> (Tank D (Listof D)))))
(define (head-n n)
  
  (: head-n-accum (Integer (Listof D) -> (Tank D (Listof D))))
  (define (head-n-accum n accum)
    
    (: step (Integer (Listof D) -> ((Stream D) -> (Tank D (Listof D)))))
    (define (step n accum)
      (λ: ((s : (Stream D)))
	  (cond
	   [(eq? s 'Nothing) (Continue  (step n accum))]
	   [(eq? s 'EOS)     (Done 'EOS (reverse accum))]
	   [else             (head-n-accum (sub1 n) (cons s accum))])))
    
    (if (zero? n)
        (Done 'Nothing (reverse accum))
        (Continue (step n accum))))
  
  (head-n-accum n '()))

(: set-tank (All (D) (-> (Tank D (Setof D)))))
(define (set-tank)
  
  (: step ((Setof D) -> ((Stream D) -> (Tank D (Setof D)))))
  (define (step the-set)
    (λ: ((datum : (Stream D)))
	(cond 
	 [(eq? datum 'Nothing)
	  (Continue (step the-set))]
	 [(eq? datum 'EOS)
	  (Done 'EOS the-set)]
	 [else (Continue (step (set-add the-set datum)))])))
  
  (Continue (step (set))))

(: list-tank (All (D) (-> (Tank D (Listof D)))))
(define (list-tank)
  
  (: step ((Listof D) -> ((Stream D) -> (Tank D (Listof D)))))
  (define (step lst)
    (λ: ((s : (Stream D)))
	(cond
	 [(eq? s 'Nothing)
	  (Continue (step lst))]
	 [(eq? s 'EOS)
	  (Done 'EOS (reverse lst))]
	 [else (Continue (step (cons s lst)))])))
  
  (Continue (step '())))

(: hash-tank (All (K V) (-> (Tank (Pair K V) (HashTable K V)))))
(define (hash-tank)
  
  (define: hmap : (HashTable K V) (make-hash))
  
  (: step ((Stream (Pair K V)) -> (Tank (Pair K V) (HashTable K V))))
  (define step
    (λ: ((s : (Stream (Pair K V))))
	(cond
	 [(eq? s 'Nothing)
	  (Continue step)]
	 [(eq? s 'EOS)
	  (Done 'EOS hmap)]
	 [else (begin
		 (hash-set! hmap (car s) (cdr s))
		 (Continue step ))])))
  
  (Continue step))


(: vector-tank (All (D) ((Vectorof D) -> (Tank D (Vectorof D)))))
(define (vector-tank vect)

  (define: vect-len : Index (vector-length vect))

  (: step (Index -> ((Stream D) -> (Tank D (Vectorof D)))))
  (define (step idx)
    (λ: ((s : (Stream D)))
	(cond
	 [(eq? s 'Nothing)
	  (Continue (step idx))]
	 [(eq? s 'EOS)
	  (Done 'EOS vect)]
	 (else 
	  (if (>= idx vect-len)
	      (Done s vect)
	      (begin
		(vector-set! vect idx s)		 
		(Continue (step (assert (add1 idx) index?)))))))))

  (Continue (step 0)))

;; Predicate Tanks

;; All datum elements satisfy the given predicate
(: and-iteratee (All (D) ((D -> Boolean) -> (Tank D Boolean))))
(define (and-iteratee pred)
  
  (: step ((Stream D) -> (Tank D Boolean)))
  (define (step input)
    (cond
     [(eq? input 'Nothing)  (Continue step)]
     [(eq? input 'EOS)      (Done 'EOS #t)]
     [else (if (pred input) (Continue step) (Done input #f))]))
  
  (Continue step))

(: sum (-> (Tank Number Number)))
(define (sum)
  (: step (Number -> ((Stream Number) -> (Tank Number Number))))
  (define (step total)
    (λ: ((str : (Stream Number)))
	(cond
	 ([number? str] (Continue (step (+ str total))))
	 [(eq? str 'Nothing) (Continue (step total))]
	 [(eq? str 'EOS)     (Done 'EOS total)])))
  (Continue (step 0)))

(: sum-i (-> (Tank Integer Integer)))
(define (sum-i)
  (: step (Integer -> ((Stream Integer) -> (Tank Integer Integer))))
  (define (step total)
    (λ: ((str : (Stream Integer)))
	(cond
	 ([number? str] (Continue (step (+ str total))))
	 [(eq? str 'Nothing) (Continue (step total))]
	 [(eq? str 'EOS)     (Done 'EOS total)])))
  (Continue (step 0)))

(: sum-r (-> (Tank Real Real)))
(define (sum-r)
  (: step (Real -> ((Stream Real) -> (Tank Real Real))))
  (define (step total)
    (λ: ((str : (Stream Real)))
	(cond
	 ([real? str] (Continue (step (+ str total))))
	 [(eq? str 'Nothing) (Continue (step total))]
	 [(eq? str 'EOS)     (Done 'EOS total)])))
  (Continue (step 0.0)))


;; Summing 1 million Ints, 424ms sum, 248ms sum1
(: sum1-i (-> (Tank Integer Integer)))
(define (sum1-i)
  
  (: total Integer)
  (define total 0)
  
  (: step ((Stream Integer) -> 
	   (Tank Integer Integer)))
  (define (step str)
    (cond
     ([number? str] 
      (set! total (+ str total))
      (Continue step))
     [(eq? str 'Nothing) (Continue step)]
     [(eq? str 'EOS) (Done 'EOS total)]))
  
  (Continue step))

(: dev/null (All (D) (-> (Tank D Void))))
(define (dev/null)
  (: step ((Stream D) -> (Tank D Void)))
  (define step
    (λ: ((str : (Stream D)))
	(if (eq? str 'EOS)
	    (Done 'EOS (void))
	    (Continue step))))
  (Continue step))

(: show (All (D) (Output-Port -> (Tank D Void))))
(define (show outp)
  (: step ((Stream D) -> (Tank D Void)))
  (define step
    (λ: ((str : (Stream D)))
	(match str
	       ('Nothing (Continue step))
	       ('EOS     (Done 'EOS (void)))
	       (s        (begin
			   (display s outp)
			   (Continue step))))))
  (Continue step))

(: print (All (D) (Output-Port -> (Tank D Void))))
(define (print outp)
  (: step ((Stream D) -> (Tank D Void)))
  (define step
    (λ: ((str : (Stream D)))
	(match str
	       ('Nothing (Continue step))
	       ('EOS     (Done 'EOS (void)))
	       (s        (begin
			   (write s outp)
			   (Continue step))))))
  (Continue step))
