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

#lang typed/racket ;; Fixme RPR - no typed/racket/base as syntax-case

(provide
 dp~ drain drain~ 
 iseq 
 Pump Hose Tank Stream
 (struct-out Continue)
 (struct-out Done)
 eseq)

(require racket/match)

(define-type (Stream D) (U D 'Nothing 'EOS))

(define-type (Tank D A) (U (Done D A) (Continue D A)))

(struct: (D A) Done ([stream : (Stream D)]
                     [accum : A]))

(struct: (D A) Continue ([step : ((Stream D) -> (Tank D A))]))	 

(: drain (All (D A) (Tank D A) -> A))
(define (drain tank)
  (match tank
    [(Done _ accum)  accum]
    [(Continue step) (drain (step 'EOS))]))

(define-syntax (dp~ stx)
  (syntax-case stx ()
    ((i~ e1 e2)
     #'(e1 e2))    
    ((i~ e1 e2 ...)
     #'(e1 (i~ e2 ...)))))

(define-syntax (drain~ stx)
(syntax-case stx ()
  ((drain~ e1 ...)
   #'(drain (dp~ e1 ...)))))

(: iseq (All (D A B) ((Tank D A) (A -> (Tank D B)) -> (Tank D B))))
(define (iseq iter fn)
  (match iter
    [(Done d a) (fn a)]
    [(Continue step) (Continue (λ: ((d : (Stream D))) 
                                 (iseq (step d) fn)))]))

(define-type (Pump D A) ((Tank D A) -> (Tank D A)))

(: eseq (All (D A) (Pump D A) (Pump D A) -> (Pump D A)))
(define (eseq e1 e2)
  (λ (iter) (e2 (e1 iter))))

(define-type (Hose O I A) ((Tank I A) -> (Tank O A)))
