Code to go along with a posting to the [haskell-cafe](https://mail.haskell.org/pipermail/haskell-cafe/2016-June/124193.html) mailing list.

I'm trying to write a mutable hash table library that would work both in the IO and ST monads, so I'm using the PrimMonad typeclass [see JmoyHash.hs  in the attached archive].

For efficiency, I would like the functions to be specialized to the concrete monad at the call site. According to Section 9.31.9.2 of the GHC User's Guide

The optimiser also considers each imported INLINABLE overloaded function, and specialises it for the different types at which it is called in M.

So I marked all the functions in my library as INLINABLE. Yet adding a SPECIALIZE pragma in Main.hs (currently commented out) for an imported function improves runtime performance by 3x, which should not be happening since, if I understand the manual right, the function should have been specialized anyway since it is marked INLINABLE.

I'm using GHC 8.0.1 with the -O2 flag.

Marking the function as INLINE might solve the problem but that's something I don't want to do as it seems to me that specialization and not inlining is what's appropriate here.

