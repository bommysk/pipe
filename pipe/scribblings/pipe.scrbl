#lang scribble/manual

@title{Data Pipes (Iteratees)}

@(require (for-label pipe/types)
  	  (for-label (only-meta-in 0 typed/racket)))

Iteratees may be viewed as accumulating part of a data pipe or a fold operation.
A minimal data pipe consists of an Enumerator associated with an Iteratee.  They are Producers and Consumers respectively.  They are also cooporating co-routines. 

An Enumerator pushes a sequence of datums, one at a time, into an Iteratee.  When either the Enumerator has no further values to send or the Iteratee determines it is done (say it "takes" or consumes exactly 5 values and no more) the Enumerator/Iteratee pipe returns some accumulated value from the Iteratee.  Hence the perspective of viewing an Enumerator/Iteratee combination as a "fold".

So for example a Enumerator of a list of Integer could be associated with an Iteratee that sums Integer values resulting in the sum total of the Integer list.

Enumeratees are one or more pipe sections that sit between an Enumerator and an Iteratee.  From the perspective of the Enumerator an Enumeratee acts as an Iteratee.  From the perspective of the Iteratee the Enumeratee acts as an Enumerator.

Enumerator -> Enumeratee1 -> ... -> EnumerateeN -> Iteratee

As the traditional terminology of Enumerator, Enumeratee and Iteratee is not very user friendly, this implementation opts for less imposing Pump, Hose and Tank metaphor respectively.

Pump -> Hose1 -> ... -> HoseN -> Tank

So for example, a Pump of a list of string feeds string values into a Hose that converts strings to integers and in turn the feeds those integers into an integer summing Tank.

A Pump combinator function takes two Pumps and combines them into a single Pump.  Say combining two Pumps of string lists to act as a single Pump of the combined lists.  Or combining two or more Pumps of text files to act as if all the files had been cat'd together.

Similarly Hoses could be nested vertically (one within the other) or joined horizontally with one piping data from one to the other.

Tanks can also be nested.

The basic combining step however is to combine a Hose with a Tank to produce a new Tank.  That Tank in turn may be repeatably combined with N number of Hoses producing an ever increasing Tank.  Finally to complete the data pipe the built up Tank is associated with a Pump and then data pipe is run to completion at which point an accumulated value is drained from the Tank.

@section{Data Hose Types}

A @racket[Tank] is an accumulator.  It is fed datum values one at a time until receiving the @racket['EOS] datum value or the Tank itself determines it is done at which point the accumulated value is drained from the @racket[Tank] and returned.

@defmodule[pipe/types]{
 
  @defthing[Stream (U D 'Nothing 'EOS)]{
    A @racket[Stream] is a parameterized type of @racket[D] datum
    or the symbols @racket['Nothing] or @racket['EOS]

    So for example, a (@racket[Stream] @racket[String]).
    @racket['EOS] denotes an end of stream marker.  @racket['Nothing]
    is a no-op step, and may be viewed as a 'null' datum value.  The
    envisioned utility of supporting a no-op step is primarily in
    asynch IO @racket[Tank]s. }

  @defthing[Tank (U (Done D A) (Continue D A))]{ }

  @defstruct*[Done ([stream (Stream D)] [accum A])]{ The @racket[Done]
    structure is the return or completion value of an @racket[Tank].
    Generally an @racket[Tank] completes when an @racket['EOS] marker
    is feed to the @racket[Tank] from somewhere upstream.  }
	
  @defstruct*[Continue ([step ((Stream D) -> (Tank D A))])]{ The
    @racket[Continue] structure holds the Tank's continuation or
    callback procedure.  To feed the next datum from the stream into
    the tank the callback @racket[step] procedure is invoked with the
    datum.}

  @defproc[(drain [tank (Tank D A)]) A]{ Extract the accumulated
    valued from a @racket[Tank].  If the @racket[Tank] is
    @racket['Done] return the accumulated value.  If the @racket[Tank]
    is ready to @racket['Continue], then @racket[drain] feeds a
    @racket['EOS], which moves the @racket[Tank] to a @racket['Done]
    state and then extracts the accumulated value.  }

}



