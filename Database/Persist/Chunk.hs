{-# LANGUAGE TypeFamilies #-}
module Database.Persist.Chunk
  ( selectSourceChunked
  , selectSourceChunkedReverse
  , selectKeysChunked
  , selectKeysChunkedReverse
  ) where

import Database.Persist
import qualified Data.Conduit                 as C
import qualified Data.Conduit.Combinators     as C
import qualified Data.Conduit.List            as CL
import Control.Monad (unless)

-- | Similar to 'Data.Conduit.Combinators.length', but implemented as an
--   accumulator instead of a fold
lengthAccumC :: (Monad m, Num len) => C.ConduitM i i m len
lengthAccumC = CL.mapAccum (\i l -> (l + 1, i)) 0

-- | Entities are fetched in chunks using 'OffsetBy' and 'LimitTo'
-- TODO: Could this be used lazily? e.g.
-- >>> take 10 $ selectSourceChunked 5 True [] :: Entity Foo
selectSourceChunked :: ( PersistEntity val
                       , PersistEntityBackend val ~ PersistMonadBackend m
                       , PersistQuery m
                       )
                    => Int                -- ^ Chunk size
                    -> [Filter val]
                    -> C.Source m (Entity val)
selectSourceChunked chunk filters = loop 0
  where
    loop off = do
      l <- selectSource filters [OffsetBy off, LimitTo chunk] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Entities are fetched in chunks using 'OffsetBy' and 'LimitTo', in reverse
selectSourceChunkedReverse :: ( PersistEntity val
                              , PersistEntityBackend val ~ PersistMonadBackend m
                              , PersistQuery m
                              )
                           => Int                -- ^ Chunk size
                           -> [Filter val]
                           -> C.Source m (Entity val)
selectSourceChunked chunk filters = do
  total <- P.count filters
  loop 0
  where
    loop off = do
      let remaining = total - off
      l <- selectSource filters [OffsetBy (max 0 $ remaining - chunk), LimitTo $ min chunk remaining] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Keys are fetched in chunks using 'OffsetBy' and 'LimitTo'
selectKeysChunked :: ( PersistEntity val
                     , PersistEntityBackend val ~ PersistMonadBackend m
                     , PersistQuery m
                     )
                  => Int                -- ^ Chunk size
                  -> [Filter val]
                  -> C.Source m (Key val)
selectKeysChunked chunk filters = loop 0
  where
    loop off = do
      l <- selectKeys filters [OffsetBy off, LimitTo chunk, Asc persistIdField] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Keys are fetched in chunks using 'OffsetBy' and 'LimitTo'
selectKeysChunkedReverse :: ( PersistEntity val
                            , PersistEntityBackend val ~ PersistMonadBackend m
                            , PersistQuery m
                            )
                         => Int                -- ^ Chunk size
                         -> [Filter val]
                         -> C.Source m (Key val)
selectKeysChunked chunk filters = loop 0
  where
    loop off = do
      l <- selectKeys filters [OffsetBy off, LimitTo chunk, Asc persistIdField] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

