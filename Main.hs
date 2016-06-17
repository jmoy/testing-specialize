module Main where

import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Hashable
import Control.Monad 
import Control.Monad.ST
import Criterion.Main
import qualified JmoyHash as J

{-{-# SPECIALIZE J.insertWith::(Hashable k,Eq k)
            => J.HashTable s k v
               -> (v->v->v) -> k -> v 
               -> ST s () #-}-}

main = defaultMain [
  env (setupWlist "text.txt") $ \wlist ->
  bgroup "wc" [bench "jh" $ nf wcJH wlist
              ]]
  
setupWlist::String->IO [S.ByteString]
setupWlist file
  = tokenise <$> S.readFile file
  where
    tokenise buf = [S.map toLower w |
                    w <- S.splitWith (not . isAlpha) buf,
                    (not . S.null) w]

wcJH::[S.ByteString]->[(S.ByteString,Int)]
wcJH ws = runST $ do
  ht <- J.new
  forM_ ws $ \w-> J.insertWith ht (+) w 1
  J.toList ht
