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
 [pipe-index      (All (O A) (Index -> (Pipe O (Pair O Index) A)))]
 [pipe-split      (All (O I A) (O -> (Listof I)) -> (Pipe O (Listof I) A))]
 [pipe-for-each   (All (D A) ((D -> Void) -> (Pipe D D A)))]
 [pipe-map        (All (O I A) ((O -> (Option I)) -> (Pipe O I A)))]
 [pipe-flatmap    (All (O I A) ((O -> (Option I)) -> (Pipe (Listof O) I A)))]
 [pipe-group      (All (D A) ((D D -> Boolean) -> (Pipe D (Listof D) A)))]
 [pipe-ungroup    (All (D A) (-> (Pipe (Listof D) D A)))]
 [pipe-unique     (All (D A) (D -> (Pipe D D A)))]
 [pipe-filter     (All (D A) ((D -> Boolean) -> (Pipe D D A)))]
 [pipe-list-count (All (D A) -> (Pipe (Listof D) (Listof (Pair D Natural)) A))]
 [pipe-filter/map (All (O I A) ((O -> (Option I)) -> (Pipe O I A)))]
 [pipe-reduce     (All (O I A) (((Listof O) -> (Option I)) -> (Pipe (Listof O) I A)))]
 [pipe-sort       (All (D A) (D D -> Boolean) -> (Pipe (Listof D) (Listof D) A))])

(require
 racket/match
 (only-in "types.rkt"
	  drain
	  Pipe
	  Tank Stream Done Done? Continue))

;; (: pipe-label (All (O K V A) (O -> (Pair K V)) -> (Pipe O (Pair K V) A)))
;; (define (pipe-label kv-fn)
;;   (λ: ((inner : (Tank (Pair K V) A)))

;;       (: step ((Tank (Pair K V)) A))
;;       )

;; )

(: pipe-split (All (O I A) (O -> (Listof I)) -> (Pipe O (Listof I) A)))
(define (pipe-split splitter)
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

(: pipe-for-each (All (D A) ((D -> Void) -> (Pipe D D A))))
(define (pipe-for-each for-fn)
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

(: pipe-filter/map (All (O I A) ((O -> (Option I)) -> (Pipe O I A))))
(define (pipe-filter/map filter/map)
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

(: pipe-map (All (O I A) ((O -> (Option I)) -> (Pipe O I A))))
(define (pipe-map map-fn)
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

(: pipe-flatmap (All (O I A) ((O -> (Option I)) -> (Pipe (Listof O) I A))))
(define (pipe-flatmap reducer)

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

(: pipe-unique (All (D A) (D -> (Pipe D D A))))
(define (pipe-unique nada)
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

(: pipe-filter (All (D A) ((D -> Boolean) -> (Pipe D D A))))
(define (pipe-filter filter-fn)

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

(: pipe-ungroup (All (D A) (-> (Pipe (Listof D) D A))))
(define (pipe-ungroup)

  (λ: ((inner : (Tank D A)))

      (: step ((Tank D A) -> ((Stream (Listof D)) -> (Tank (Listof D) A))))
      (define (step inner)
	(λ: ((data : (Stream (Listof D))))
	    (cond
	     ((eq? data 'Nothing)
	      (Continue (step inner)))
	     ((eq? data 'EOS)
	      (Done 'EOS (drain inner)))
	     (else (let loop ((data data) (inner inner))
		     (if (null? data)
			 (Continue (step inner))
			 (match inner
				[(Done _ _)
				 (Done data (drain inner))]
				[(Continue istep)
				 (loop (cdr data) (istep (car data)))])))))))

      (Continue (step inner))))


(: pipe-group (All (D A) ((D D -> Boolean) -> (Pipe D (Listof D) A))))
(define (pipe-group comparer)

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

(: pipe-sort (All (D A) (D D -> Boolean) -> (Pipe (Listof D) (Listof D) A)))
(define (pipe-sort compare)

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

(: pipe-list-count (All (D A) -> (Pipe (Listof D) (CntL D) A)))
(define (pipe-list-count)
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

;; (: enumeratee-unfold (All (O I A) ((O -> (Listof I)) -> (Pipe O I A))))
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
(: pipe-reduce (All (O I A) (((Listof O) -> (Option I)) -> (Pipe (Listof O) I A))))
(define (pipe-reduce reduce)
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

(: pipe-index (All (O A) (Index -> (Pipe O (Pair O Index) A))))
(define (pipe-index start-at)
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

;; FIXME RPR - Really need a Pipe compose function.
;; (: groupby-aggregate (All (D E A) ((D D -> Boolean) ((Listof D) -> (Option E)) -> (Pipe D E A))))
;; (define (reduce is-same? reduce-fn)
;;   (define: groupbyT : (Pipe D (Listof D) (Listof E))
;;     (pipe-groupby is-same?))
;;   (define: mergeT : (Pipe (Listof D) E (Listof E))
;;     (pipe-reduce reduce-fn))
;;   (define: sink : (Tank E (Listof E))
;;     (list-sink))
;;   (define: list-iteratee : (Tank D (Listof E))
;;     (groupbyT (mergeT sink)))
;;   list-iteratee)
