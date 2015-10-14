{-# LANGUAGE FlexibleContexts, TypeFamilies #-}
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
import qualified Data.Conduit.List            as CL
import qualified Data.Map.Strict              as Map
import Data.Map.Strict                                (Map)
import Control.Monad.Trans.Resource (MonadResource)
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Reader (MonadReader)
import Control.Monad.IO.Class (MonadIO)

-- | Similar to 'Data.Conduit.Combinators.length', but implemented as an
--   accumulator instead of a fold
lengthAccumC :: (Monad m, Num len) => C.ConduitM i i m len
lengthAccumC = CL.mapAccum (\i l -> (l + 1, i)) 0

-- | Entities are fetched in chunks using 'OffsetBy' and 'LimitTo'
-- TODO: Could this be used lazily? e.g.
-- >>> take 10 $ selectSourceC 5 True [] :: Entity Foo
selectSourceC
  :: (HasPersistBackend env (PersistEntityBackend val),PersistQuery (PersistEntityBackend val),PersistEntity val,MonadResource m,MonadReader env m)
  => Int -- ^ Chunk size
  -> [Filter val]
  -> C.ConduitM () (Entity val) m ()
selectSourceC chunk filters = loop 0
  where
    loop off = do
      l <- selectSource filters [OffsetBy off, LimitTo chunk] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Keys are fetched in chunks using 'OffsetBy' and 'LimitTo'
selectKeysC
  :: (MonadReader env m,MonadResource m,HasPersistBackend env (PersistEntityBackend val),PersistEntity val,PersistQuery (PersistEntityBackend val))
  => Int -- ^ Chunk size
  -> [Filter val]
  -> C.ConduitM () (Key val) m ()
selectKeysC chunk filters = loop 0
  where
    loop off = do
      l <- selectKeys filters [OffsetBy off, LimitTo chunk, Asc persistIdField] C.$= lengthAccumC
      unless (l < chunk) $ loop (off + chunk)

-- | Efficient query for fetching multiple entities by key, maintaining the original order and with Nothing to fill holes (where nothing was found)
getInManyListC
  :: (MonadIO m,PersistEntity b,PersistQuery (PersistEntityBackend b),Functor m)
  => Int -> [Key b] -> ReaderT (PersistEntityBackend b) m [Maybe b]
getInManyListC chunk ks = map (fmap entityVal) . map listToMaybe <$> selectInManyListC' chunk (idField, entityKey) ks
  where
    proxy              = joinProxy ks
    proxyEntityField   = ((\_ -> Proxy) :: Proxy e -> Proxy (EntityField e (Key e))) proxy
    idField            = persistIdField `asProxyTypeOf` proxyEntityField

-- | Helper for 'selectInManyListC' and 'getInManyListC'
selectInManyListC'
  :: (Ord k,MonadIO m,PersistEntity val,PersistField k,PersistQuery (PersistEntityBackend val),Functor m)
  => Int
  -> (EntityField val k,Entity val -> k)
  -> [k]
  -> ReaderT (PersistEntityBackend val) m [[Entity val]]
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
selectInManyListC
  :: (Ord k,PersistQuery (PersistEntityBackend b),PersistField k,PersistEntity b,MonadIO m,Functor m)
  => Int -- ^ Chunk size
  -> (EntityField b k,b -> k)
  -> [k]
  -> ReaderT (PersistEntityBackend b) m [[Entity b]]
selectInManyListC chunk (field, accessor) = selectInManyListC' chunk (field, accessor . entityVal)

joinProxy :: proxy (proxy' e) -> Proxy e
joinProxy _ = Proxy
