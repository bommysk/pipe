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

(require
 (only-in "types.rkt"
	  Pipe Tank)
 (only-in "pipes.rkt"
	  pipe-groupby pipe-flatmap)
 (only-in "tanks.rkt"
	  list-tank))

(provide:
 [reduce (All (D E) ((D D -> Boolean) (D -> (Option E)) -> (Tank D (Listof E))))])

(: reduce (All (D E) ((D D -> Boolean) (D -> (Option E)) -> (Tank D (Listof E)))))
(define (reduce is-same? reduce-fn)
  (define: groupbyT : (Pipe D (Listof D) (Listof E))
    (pipe-groupby is-same?))
  (define: mergeT : (Pipe (Listof D) E (Listof E))
    (pipe-flatmap reduce-fn))
  (define: sink : (Tank E (Listof E))
    (list-tank))
  (define: listtank : (Tank D (Listof E))
    (groupbyT (mergeT sink)))
  listtank)
