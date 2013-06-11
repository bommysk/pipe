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
 [hose-index      (All (O A) (Index -> (Hose O (Pair O Index) A)))]
 [hose-split      (All (O I A) (O -> (Listof I)) -> (Hose O (Listof I) A))]
 [hose-for-each   (All (D A) ((D -> Void) -> (Hose D D A)))]
 [hose-map        (All (O I A) ((O -> (Option I)) -> (Hose O I A)))]
 [hose-flatmap    (All (O I A) ((O -> (Option I)) -> (Hose (Listof O) I A)))]
 [hose-groupby    (All (D A) ((D D -> Boolean) -> (Hose D (Listof D) A)))]
 [hose-unique     (All (D A) (D -> (Hose D D A)))]
 [hose-filter     (All (D A) ((D -> Boolean) -> (Hose D D A)))]
 [hose-list-count (All (D A) -> (Hose (Listof D) (Listof (Pair D Natural)) A))]
 [hose-filter/map (All (O I A) ((O -> (Option I)) -> (Hose O I A)))]
 [hose-reduce     (All (O I A) (((Listof O) -> (Option I)) -> (Hose (Listof O) I A)))]
 [hose-sort       (All (D A) (D D -> Boolean) -> (Hose (Listof D) (Listof D) A))])

(require
 racket/match
 (only-in "types.rkt"
	  drain
	  Hose
	  Tank Stream Done Continue))


(: hose-split (All (O I A) (O -> (Listof I)) -> (Hose O (Listof I) A)))
(define (hose-split splitter)
  (λ: ((inner : (Tank (Listof I) A)))

      (: step ((Tank (Listof I) A) -> ((Stream O) -> (Tank O A))))
      (define (step inner)
	(λ: ((elem : (Stream O)))
	    (cond
	     [(eq? elem 'Nothing)
	      (Continue (step inner))]
	     [(eq? elem 'EOS)
	      (Done 'EOS (drain inner))]
	     [else (match inner
			  [(Done _ _)
			   (Done elem (drain inner))]
			  [(Continue istep)
			   (Continue (step (istep (splitter elem))))])])))

      (Continue (step inner))))

(: hose-for-each (All (D A) ((D -> Void) -> (Hose D D A))))
(define (hose-for-each for-fn)
  (λ: ((inner : (Tank D A)))

      (: step ((Tank D A) -> ((Stream D) -> (Tank D A))))
      (define (step inner)
	(λ: ((elem : (Stream D)))
	    (cond
	     [(eq? elem 'Nothing)
	      (Continue (step inner))]
	     [(eq? elem 'EOS)
	      (Done 'EOS (drain inner))]
	     [else (match inner
			  [(Done _ _)
			   (Done elem (drain inner))]
			  [(Continue istep)
			   (begin
			     (for-fn elem)
			     (Continue (step (istep elem))))])])))

      (Continue (step inner))))

(: hose-filter/map (All (O I A) ((O -> (Option I)) -> (Hose O I A))))
(define (hose-filter/map filter/map)
  (λ: ((inner : (Tank I A)))

      (: step ((Tank I A) -> ((Stream O) -> (Tank O A))))
      (define (step inner)
	(λ: ((elem : (Stream O)))
	    (cond
	     ((eq? elem 'Nothing) (Continue (step inner)))
	     ((eq? elem 'EOS)     (Done 'EOS (drain inner)))
	     (else                (match inner
					 [(Done _ _ ) (Done elem (drain inner))]
					 [(Continue istep)
					  (let ((new-elem (filter/map elem)))
					    (if new-elem
						(let ((newinner (istep new-elem)))
						  (Continue (step newinner)))
						(Continue (step inner))))])))))

      (Continue (step inner))))

(: hose-map (All (O I A) ((O -> (Option I)) -> (Hose O I A))))
(define (hose-map map-fn)
  (λ: ((inner : (Tank I A)))

      (: step ((Tank I A) -> ((Stream O) -> (Tank O A))))
      (define (step inner)
	(λ: ((elem : (Stream O)))
	    (cond
	     ((eq? elem 'Nothing) (Continue (step inner)))
	     ((eq? elem 'EOS)     (Done 'EOS (drain inner)))
	     (else                (match inner
					 [(Done _ _ )
					  (Done elem (drain inner))]
					 [(Continue istep)
					  (let ((iota (map-fn elem)))
					    (if iota
						(Continue (step (istep iota)))
						(Continue (step inner))))])))))

      (Continue (step inner))))

(: hose-flatmap (All (O I A) ((O -> (Option I)) -> (Hose (Listof O) I A))))
(define (hose-flatmap reducer)

  (λ: ((inner : (Tank I A)))

      (: step ((Tank I A) -> ((Stream (Listof O)) -> (Tank (Listof O) A))))
      (define (step inner)
	(λ: ((datum : (Stream (Listof O))))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step inner)))
	     ((eq? datum 'EOS)
	      (Done 'EOS (drain inner)))
	     (else
	      (let: loop : (Tank (Listof O) A)
		    ([data : (Listof O) datum]
		     [inner : (Tank I A) inner])
		    (if (null? data)
			(Continue (step inner))
			(match inner
			       [(Done _ _) (Done data (drain inner))]
			       [(Continue istep)
				(let: ((d : (Option I) (reducer (car data))))
				      (if d
					  (loop (cdr data) (istep d))
					  (loop (cdr data) inner)))])))))))


      (Continue (step inner))))

(: hose-unique (All (D A) (D -> (Hose D D A))))
(define (hose-unique nada)
  (λ: ((inner : (Tank D A)))

      (: step ((Tank D A) D -> ((Stream D) -> (Tank D A))))
      (define (step inner last-datum)
	(λ: ((datum : (Stream D)))
	    (cond
	     [(eq? datum 'Nothing)
	      (Continue (step inner last-datum))]
	     [(eq? datum 'EOS)
	      (Done 'EOS (drain inner))]
	     [else (match inner
			  [(Done _ _)
			   (Done datum (drain inner))]
			  [(Continue istep)
			   (if (equal? datum last-datum)
			       (Continue (step inner last-datum))
			       (Continue (step (istep datum) datum)))])])))

      (Continue (step inner nada))))

(: hose-filter (All (D A) ((D -> Boolean) -> (Hose D D A))))
(define (hose-filter filter-fn)

  (λ: ((inner : (Tank D A)))

      (: step ((Tank D A) -> ((Stream D) -> (Tank D A))))
      (define (step inner)
	(λ: ((elem : (Stream D)))
	    (cond
	     [(eq? elem 'Nothing)
	      (Continue (step inner))]
	     [(eq? elem 'EOS)
	      (Done 'EOS (drain inner))]
	     [else (match inner
			  [(Done _ _)
			   (Done elem (drain inner))]
			  [(Continue istep)
			   (if (filter-fn elem)
			       (Continue (step (istep elem)))
			       (Continue (step inner)))])])))

      (Continue (step inner))))

(: hose-groupby (All (D A) ((D D -> Boolean) -> (Hose D (Listof D) A))))
(define (hose-groupby comparer)

  (λ: ((inner : (Tank (Listof D) A)))

      (: step ((Listof D) (Tank (Listof D) A) -> ((Stream D) -> (Tank D A))))
      (define (step accum inner)
	(λ: ((datum : (Stream D)))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step accum inner)))
	     ((eq? datum 'EOS)
	      (match inner
		     [(Done _ _ ) (Done 'EOS (drain inner))]
		     [(Continue inner-step)
		      (Done 'EOS (drain (inner-step accum)))]))
	     (else (match inner
			  [(Done _ _) (Done datum (drain inner))]
			  [(Continue inner-step)
			   (if (null? accum)
			       (Continue (step (list datum) inner))
			       (if (comparer datum (car accum))
				   (Continue (step (cons datum accum) inner))
				   (Continue (step (list datum) (inner-step accum)))))])))))

      (Continue (step '() inner))))

(: hose-sort (All (D A) (D D -> Boolean) -> (Hose (Listof D) (Listof D) A)))
(define (hose-sort compare)

  (λ: ((inner : (Tank (Listof D) A)))

      (: step ((Tank (Listof D) A) -> ((Stream (Listof D)) -> (Tank (Listof D) A))))
      (define (step inner)
	(λ: ((datum : (Stream (Listof D))))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step inner)))
	     ((eq? datum 'EOS)
	      (Done 'EOS (drain inner)))
	     (else
	      (match inner
		     [(Done _ _) (Done datum (drain inner))]
		     [(Continue inner-step)
		      (if (null? datum)
			  (Continue (step inner))
			  (let: ((datum-sorted : (Listof D) (sort datum compare)))
				(Continue (step (inner-step datum-sorted)))))])))))

      (Continue (step inner))))

(define-type (CntL D) (Listof (Pair D Natural)))

(: list-count (All (D) (Listof D) -> (CntL D)))
(define (list-count lst)
  (let: ((ht : (HashTable D Natural) (make-hash)))
	(let loop ((lst lst))
	  (if (null? lst)
	      (hash->list ht)
	      (begin
		(hash-update! ht (car lst) (λ: ((cnt : Natural)) (add1 cnt)) (λ: () 0))
		(loop (cdr lst)))))))

(: hose-list-count (All (D A) -> (Hose (Listof D) (CntL D) A)))
(define (hose-list-count)
  (λ: ((inner : (Tank (Listof (Pair D Natural)) A)))
      (: step ((Tank (CntL D) A) -> ((Stream (Listof D)) -> (Tank (Listof D) A))))
      (define (step inner)
	(λ: ((datum : (Stream (Listof D))))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step inner)))
	     ((eq? datum 'EOS)
	      (Done 'EOS (drain inner)))
	     (else
	      (match inner
		     [(Done _ _) (Done datum (drain inner))]
		     [(Continue inner-step)
		      (let: ((datum-counts : (CntL D) (list-count datum)))
			    (Continue (step (inner-step datum-counts))))])))))

      (Continue (step inner))))

;; (: enumeratee-unfold (All (O I A) ((O -> (Listof I)) -> (Hose O I A))))
;; (define (enumeratee-unfold f-cvt)

;;   (λ: ((inner : (Tank I A)))

;;       (: iter-elems ((Tank I A) O -> (Tank I A)))
;;       (define (iter-elems rec-iter elem)
;;	(let: loop : (Tank I A) ((iter : (Tank I A) rec-iter)
;;				     (elems : (Listof I) (f-cvt elem)))
;;	      (if (null? elems)
;;		  iter
;;		  (match iter
;;			 [(Done _ _) iter]
;;			 [(Continue istep)
;;			  (loop (istep (car elems)) (cdr elems))]))))

;;       (: step ((Tank I A) -> ((Stream O) -> (Tank O (Tank I A)))))
;;       (define (step inner)
;;	(λ: ((elem : (Stream O)))
;;	    (cond
;;	     ((eq? elem 'Nothing)
;;	      (Continue (step inner)))
;;	     ((eq? elem 'EOS)
;;	      (Done 'EOS inner))
;;	     (else (match inner
;;			  [(Done _ _ ) (Done elem inner)]
;;			  [(Continue _)
;;			   (let ((rec-iter (iter-elems inner elem)))
;;			     (match rec-iter
;;				    [(Done _ _) (Done elem rec-iter)]
;;				    [(Continue _)
;;				     (Continue (step rec-iter))]))])))))

;;       (Continue (step inner))))

;; A reduce hands over the entire list.
(: hose-reduce (All (O I A) (((Listof O) -> (Option I)) -> (Hose (Listof O) I A))))
(define (hose-reduce reduce)
  (λ: ((inner : (Tank I A)))

      (: step ((Tank I A) -> ((Stream (Listof O)) -> (Tank (Listof O) A))))
      (define (step inner)
	(λ: ((datum : (Stream (Listof O))))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step inner)))
	     ((eq? datum 'EOS)
	      (Done 'EOS (drain inner)))
	     (else
	      (match inner
		     [(Done _ _) (Done datum (drain inner))]
		     [(Continue inner-step)
		      (let: ((reduced-datum : (Option I) (reduce datum)))
			    (if reduced-datum
				(Continue (step (inner-step reduced-datum)))
				(Continue (step inner))))])))))

      (Continue (step inner))))

(: hose-index (All (O A) (Index -> (Hose O (Pair O Index) A))))
(define (hose-index start-at)
  (λ: ((inner : (Tank (Pair O Index) A)))

      (: step (Index (Tank (Pair O Index) A) -> ((Stream O) -> (Tank O A))))
      (define (step idx inner)
	(λ: ((datum : (Stream O)))
	    (cond
	     ((eq? datum 'Nothing)
	      (Continue (step idx inner)))
	     ((eq? datum 'EOS)
	      (Done 'EOS (drain inner)))
	     (else
	      (match inner
		     [(Done _ _) (Done datum (drain inner))]
		     [(Continue inner-step)
		      (Continue (step (assert (add1 idx) index?)
				      (inner-step (cons datum idx))))])))))

      (Continue (step start-at inner))))

;; FIXME RPR - Really need a Hose compose function.
;; (: groupby-aggregate (All (D E A) ((D D -> Boolean) ((Listof D) -> (Option E)) -> (Hose D E A))))
;; (define (reduce is-same? reduce-fn)
;;   (define: groupbyT : (Hose D (Listof D) (Listof E))
;;     (hose-groupby is-same?))
;;   (define: mergeT : (Hose (Listof D) E (Listof E))
;;     (hose-reduce reduce-fn))
;;   (define: sink : (Tank E (Listof E))
;;     (list-sink))
;;   (define: list-iteratee : (Tank D (Listof E))
;;     (groupbyT (mergeT sink)))
;;   list-iteratee)
