{-# LANGUAGE BangPatterns,RankNTypes #-}
module JmoyHash where

import GHC.Prim (RealWorld)
import Control.Monad
import Control.Monad.ST
import Control.Exception
import Control.Monad.Primitive
import Control.Applicative
import Data.Maybe
import Data.Word
import Data.Bits
import Data.Hashable
import Data.Primitive.MutVar
import qualified Data.Vector.Generic.Mutable as VM
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed.Mutable as VUM

----------------------------------------------------
-- Marks
type Mark = Word32

emptyMark = 1::Mark
isEmpty::Mark->Bool
isEmpty = (1==)

deletedMark = 2::Mark
isDeleted::Mark->Bool
isDeleted = (2==)

-- Can never be equal to emptyMark or deletedMark
{-# INLINE hashToMark #-}
hashToMark::Int->Mark
hashToMark hash 
  = let m = fromIntegral hash `shift` 2 in
      assert (not (isEmpty m || isDeleted m)) m

----------------------------------------------------
-- The Hash Table Structure
type Hash = Int

data HashTable_ s k v
  = HT  { sz::{-#UNPACK#-} !Int                     -- Allocated size of arrays
        , hwm::{-#UNPACK#-} !Int                    -- High-water mark for load
        , ld::{-#UNPACK#-} !(MutVar s Int)             -- Current number of items
        , dl::{-#UNPACK#-} !(MutVar s Int)             -- Number of deleted items
        , marks::{-#UNPACK#-} !(VUM.MVector s Mark)   -- Marks
        , keys::{-#UNPACK#-} !(VBM.MVector s k)       -- Keys
        , values::{-#UNPACK#-} !(VBM.MVector s v)     -- Values
        }

type HashTable s k v = MutVar s (HashTable_ s k v)

-- TODO: validate size before proceeding
{-# INLINABLE newSz #-}
newSz::PrimMonad m
       =>Int->m (HashTable (PrimState m) k v)
newSz n = do
  ld_ <- newMutVar 0
  dl_ <- newMutVar 0
  marks_ <- VM.replicate n emptyMark
  keys_ <- VM.new n
  values_ <- VM.new n
  newMutVar  HT{sz=n, hwm=floor(0.7*fromIntegral n),
                ld=ld_, dl=dl_,
                marks=marks_, keys=keys_, values=values_}

defSz = 4::Int

{-# INLINE new #-}
new::PrimMonad m => m (HashTable (PrimState m) k v)
new = newSz defSz

{-# INLINABLE map #-}
map::PrimMonad m
     => (v->w)->HashTable (PrimState m) k v->m (HashTable (PrimState m) k w)
map f htr = do
  ht <- readMutVar htr
  let n = sz ht
  ld_ <- readMutVar (ld ht) >>= newMutVar
  dl_ <- readMutVar (dl ht) >>= newMutVar
  marks_ <- VM.clone (marks ht)
  keys_ <- VM.clone (keys ht)
  values_ <- VM.new n
  forM_ [0..n-1] $ \i -> do
    m <- VM.read marks_ i
    when (not (isEmpty m || isDeleted m)) $ do
      v <- VM.read (values ht) i
      let !w = f v
      VM.write values_ i w
  newMutVar  HT{sz=n, hwm=hwm ht,
                ld=ld_, dl=dl_,
                marks=marks_, keys=keys_, values=values_}

{-# INLINE clone #-}
clone::PrimMonad m => HashTable (PrimState m) k v->m (HashTable (PrimState m) k v)
clone = JmoyHash.map id

{-# INLINE incLd #-}
incLd ht =
  modifyMutVar' (ld ht) (+1)

{-# INLINE decLd #-}
decLd ht = 
  modifyMutVar' (ld ht) (subtract 1)

{-# INLINE incDl #-}
incDl ht = 
  modifyMutVar' (dl ht) (+1)

{-# INLINE decDl #-}
decDl ht = 
  modifyMutVar' (dl ht) (subtract 1)

-- Check if the hash-table needs resizing
-- May return a different hash table if resize occurs
{-# INLINABLE checkResize #-}
checkResize::(Hashable k,Eq k,PrimMonad m)
           => HashTable_ (PrimState m) k v
           -> m (Maybe (HashTable (PrimState m) k v))
checkResize ht = do
  ld_ <- readMutVar (ld ht)
  if ld_ < hwm ht then
    return Nothing
  else do
    let newsz = 2*sz ht
    Just <$> (newMutVar ht >>= toList >>= fromListSz newsz)

------------------------------------------------------
-- Probing
type Slot=Int

{-# INLINE getSlot #-}
getSlot::HashTable_ s k v->Hash->Slot
getSlot ht h 
  = let n = sz ht
        s0 = h `mod` n in
        assert (s0>=0 && s0<n) s0

-- Probe sequence: linear for now
{-# INLINE probeNxt #-}
probeNxt::Slot       --  Current position
        ->Slot       --  Size of hash table
        ->Slot       --  Next position
probeNxt i sz 
  = let i' = if i>=sz-1 then 0 else i+1 in
    assert (i'>=0 && i'<sz) i'

-- Probe for a key with a given hash
{-# INLINE probe #-}
probe::(Eq k,PrimMonad m)
     => HashTable_ (PrimState m) k v
     -> k -- Key to probe
     -> Hash -- Hash of `k`
     -> m (Maybe Slot) -- Slot where `k` resides if it exists
probe ht k h
  = go s0 True
    where
      m = hashToMark h
      n = sz ht
      s0 = getSlot ht h
      go t first
        | t == s0 && not first = return Nothing
        | otherwise = do
            m' <- VM.read (marks ht) t
            if m'==m then do
              k' <- VM.read (keys ht) t
              if k'==k then
                return (Just t)
              else
                go (probeNxt t n) False
            else if isEmpty m' then
                return Nothing
            else
                go (probeNxt t n) False

{-# INLINE probe' #-}
probe'::(Eq k,PrimMonad m)
      =>HashTable_ (PrimState m) k v
      ->k -- Key to probe
      ->Hash -- Hash of `k`
      ->m (Maybe Slot -- Slot where `k` can be inserted
          ,Maybe Slot -- Slot where `k` currently exists
          )
probe' ht k h
  = go Nothing s0 True
    where
      m = hashToMark h
      n = sz ht
      s0 = getSlot ht h
      go !ip t first
        | t == s0 && not first = return (ip,Nothing)
        | otherwise = do
            m' <- VM.read (marks ht) t
            if m'==m then do
              k' <- VM.read (keys ht) t
              if k'==k then
                return (ip <|> Just t, Just t)
              else
                go ip (probeNxt t n) False
            else if isEmpty m' then
                return (ip <|> Just t, Nothing)
            else if isDeleted m' then
                go (ip <|> Just t) (probeNxt t n) False
            else
                go ip (probeNxt t n) False

-- Mark a slot as deleted
{-# INLINE wipe #-}
wipe::PrimMonad m
      => HashTable_ (PrimState m) k v
    -> Slot
    -> m ()
wipe ht i = do
  m' <- VM.read (marks ht) i
  unless (isDeleted m' || isEmpty m') $ decLd ht
  VM.write (marks ht) i deletedMark
  unless (isDeleted m') $ incDl ht
  
-- Write a new key-value pair
-- This my trigger a resize, so a different table may be
--   returned
{-# INLINE writeEntry #-}
writeEntry::(Hashable k,Eq k,PrimMonad m)
          => HashTable_ (PrimState m) k v
          -> Slot
          -> k
          -> Hash
          -> v
          -> m (Maybe (HashTable (PrimState m) k v))
writeEntry ht i k h v = do
  let m = hashToMark h
  m' <- VM.read (marks ht) i
  when (isDeleted m') $  decDl ht
  VM.write (marks ht) i m
  VM.write (keys ht) i k
  VM.write (values ht) i v
  incLd ht
  checkResize ht
  

-- This is the most fundamental single-item function
--  since all other functions that mutate a single item
--  insert, insertWith and delete can be written in terms of it
{-# INLINE alter #-}
alter::(Hashable k,Eq k,PrimMonad m)
        => HashTable (PrimState m) k v
        -> (Maybe v -> Maybe v)
        -- We look for a key `k`, if not found we pass Nothing
        --  else (Just `value`) where `value` is the current
        --  value mapped to `k`. This is passed to `f`.
        --  If a`action` returns Nothing we delete the key,
        --  otherwise if it returns (Just value') we map `k` to
        --  `value'`
        -> k -- Key to update
        -> m (Maybe v) -- The value returned by `f`
alter htr f k = do
  ht <- readMutVar htr
  let h = hash k
  (ip,old) <- probe' ht k h
  case (ip,old) of
    -- Did not find a place to insert
    -- This should not happen since we resize before
    -- load become one
    (Nothing,_) -> error "Unexpectely out of space"
    -- Found a place to insert, but there is also an old entry
    (Just i,Just o) -> do
        v' <- VM.read (values ht) o
        let u = f (Just v')
        case u of
          -- Delete old entry
          Nothing -> wipe ht o
          -- Update old entry
          Just v'' -> v'' `seq`
            if i==o then -- Just overwrite the value
              VM.write (values ht) i v''
            else do      -- Delete old entry and insert new one
              wipe ht o
              nht <- writeEntry ht i k h v''
              -- Update ref if resize happened
              case nht of
                Nothing -> return()
                Just ht' -> 
                  readMutVar ht' >>= writeMutVar htr
        return u
    -- There is a place to insert but no old entry
    (Just i,Nothing) -> do
        let u = f Nothing
        case u of
          -- Do nothing
          Nothing -> return ()
          -- Insert entry
          Just !v'' -> v'' `seq` do
            nht <- writeEntry ht i k h v''
            -- Update ref if resize happened
            case nht of
              Nothing -> return ()
              Just ht' ->
                readMutVar ht' >>= writeMutVar htr
        return u

{-# INLINABLE lookup #-}
lookup::(Hashable k,Eq k,PrimMonad m)
      => HashTable (PrimState m) k v
      -> k
      -> m (Maybe v)
lookup htr k = do
  ht <- readMutVar htr
  let h = hash k
  res <- probe ht k h 
  case res of
    Nothing -> return Nothing
    Just t -> Just <$> VM.read (values ht) t

{-# INLINABLE insertWith #-}
insertWith::(Hashable k,Eq k,PrimMonad m)
            => HashTable (PrimState m) k v
            -> (v->v->v) -> k -> v
            -> m ()
insertWith ht f k v= void $ alter ht f' k 
  where
    f' Nothing = Just v
    f' (Just v') = Just (f v' v)

{-# INLINABLE insert #-}
insert::(Hashable k,Eq k,PrimMonad m)
      => HashTable (PrimState m) k v -> k -> v
      -> m ()
insert ht k v= void $ alter ht (const (Just v)) k

{-# INLINABLE delete #-}
delete::(Hashable k,Eq k,PrimMonad m)
      => HashTable (PrimState m) k v -> k
      -> m ()
delete ht = void . alter ht (const Nothing)

{-# INLINE foldWithKey' #-}
foldWithKey'::(Hashable k,Eq k,PrimMonad m)
            => HashTable (PrimState m) k v
            -> (a->k->v->a)
            -> a
            -> m a
foldWithKey' htr f a0 = do
  ht <- readMutVar htr
  go ht 0 a0
  where
    go ht !i !accum
      |i == sz ht = return accum
      |otherwise = do
          m <- VM.read (marks ht) i
          if isEmpty m || isDeleted m then
            go ht (i+1) accum
          else do
            k <- VM.read (keys ht) i
            v <- VM.read (values ht) i
            let !accum' = f accum k v
            go ht (i+1) accum'

{-# INLINABLE toList #-}
toList::(Hashable k,Eq k,PrimMonad m)
        => HashTable (PrimState m) k v
      -> m [(k,v)]
toList htr = foldWithKey' htr (\l k v -> (k,v):l) []

{-# INLINE fromListSz #-}
fromListSz:: (Hashable k,Eq k,PrimMonad m)
          => Int
          -> [(k,v)]
          -> m (HashTable (PrimState m) k v)
fromListSz sz kvs = do
  ht <- newSz sz
  forM_ kvs $ uncurry (insert ht)
  return ht

{-# INLINE fromList #-}
fromList::(Hashable k,Eq k,PrimMonad m)
        => [(k,v)]
        -> m (HashTable (PrimState m) k v)
fromList = fromListSz defSz

  
                        

