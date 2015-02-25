{-# LANGUAGE TypeFamilies #-}
module Database.Persist.Chunk
  ( selectSourceC
  , selectKeysC
  , getInManyListC
  , selectInManyListC
  ) where

import Data.Functor                                   ((<$>))
import Data.Proxy
import Data.Maybe                                     (listToMaybe, fromMaybe)
import Control.Monad
import Control.Arrow                                  ((&&&))
import Data.List.Split                                (chunksOf)
import Database.Persist
import qualified Database.Persist.Sql         as P
import qualified Data.Conduit                 as C
import qualified Data.Conduit.Combinators     as C
import qualified Data.Conduit.List            as CL
import qualified Data.Map.Strict              as Map
import Data.Map.Strict                                (Map)
import Control.Monad                                  (unless)

-- | Similar to 'Data.Conduit.Combinators.length', but implemented as an
--   accumulator instead of a fold
lengthAccumC :: (Monad m, Num len) => C.ConduitM i i m len
lengthAccumC = CL.mapAccum (\i l -> (l + 1, i)) 0

-- | Entities are fetched in chunks using 'OffsetBy' and 'LimitTo'
-- TODO: Could this be used lazily? e.g.
-- >>> take 10 $ selectSourceC 5 True [] :: Entity Foo
selectSourceC  :: ( PersistEntity val
                  , PersistEntityBackend val ~ PersistMonadBackend m
                  , PersistQuery m
                  )
              => Int                -- ^ Chunk size
              -> [Filter val]
              -> C.Source m (Entity val)
selectSourceC chunk filters = loop 0
  where
    loop off = do
      l <- selectSource filters [OffsetBy off, LimitTo chunk] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Keys are fetched in chunks using 'OffsetBy' and 'LimitTo'
selectKeysC  :: ( PersistEntity val
                , PersistEntityBackend val ~ PersistMonadBackend m
                , PersistQuery m
                )
            => Int                -- ^ Chunk size
            -> [Filter val]
            -> C.Source m (Key val)
selectKeysC chunk filters = loop 0
  where
    loop off = do
      l <- selectKeys filters [OffsetBy off, LimitTo chunk, Asc persistIdField] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Efficient query for fetching multiple entities by key, maintaining the original order and with Nothing to fill holes (where nothing was found)
getInManyListC ::  ( Functor m
                   , Monad m
                   , P.PersistQuery m
                   , PersistMonadBackend m ~ PersistEntityBackend e
                   , PersistEntity e
                   )
              => Int                -- ^ Chunk size
              -> [Key e]
              -> m [Maybe e]
getInManyListC chunk ks = map (fmap entityVal) . map listToMaybe <$> selectInManyListC' chunk (idField, entityKey) ks
  where
    proxy              = joinProxy ks
    proxyEntityField   = ((\_ -> Proxy) :: Proxy e -> Proxy (EntityField e (Key e))) proxy
    idField            = persistIdField `asProxyTypeOf` proxyEntityField

-- | Helper for 'selectInManyListC' and 'getInManyListC'
selectInManyListC' ::  ( Functor m
                       , Monad m
                       , P.PersistQuery m
                       , PersistEntity e
                       , PersistField ftype
                       , PersistMonadBackend m ~ PersistEntityBackend e
                       , Ord ftype
                       )
                 => Int               -- ^ Chunk size
                 -> (EntityField e ftype, Entity e -> ftype)
                 -> [ftype]
                 -> m [[Entity e]]
selectInManyListC' chunk (field, accessor) vals =
  fmap concat $ forM (chunksOf chunk vals) $ \cvals -> do
    results <- P.selectList [field P.<-. cvals] []
    let fieldResults = mapFromListMulti $ map (accessor &&& id) results
    return $ flip map cvals $ \v ->
      fromMaybe [] $ Map.lookup v fieldResults
  where
    mapFromListMulti :: Ord k => [(k, a)] -> Map k [a]
    mapFromListMulti [] = Map.empty
    mapFromListMulti ((xk,xa):xs) = Map.map reverse $ insertAll (Map.singleton xk [xa]) xs
      where
        f y (Just ys) = Just $ y : ys
        f y Nothing   = Just $ [y]
        insertAll m []           = m
        insertAll m ((yk,ya):ys) = insertAll (Map.alter (f ya) yk m) ys


-- | Select in query which maintains the ordering of results
selectInManyListC ::  ( Functor m
                      , Monad m
                      , P.PersistQuery m
                      , PersistEntity e
                      , PersistField ftype
                      , PersistMonadBackend m ~ PersistEntityBackend e
                      , Ord ftype
                      )
                 => Int               -- ^ Chunk size
                 -> (EntityField e ftype, e -> ftype)
                 -> [ftype]
                 -> m [[Entity e]]
selectInManyListC chunk (field, accessor) = selectInManyListC' chunk (field, accessor . entityVal)

joinProxy :: proxy (proxy' e) -> Proxy e
joinProxy _ = Proxy
