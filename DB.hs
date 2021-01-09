module DB where

import GHC.TypeLits
import Data.Text (Text)
import Data.Proxy

-- DAL/Abstract
--

class KnownSymbol t => HasColumn t a where
  column :: (Proxy t) -> a -> Text

class HasColumns a where
  columns :: a -> [Text]

class HasTable a e where
  tablename :: e -> a -> Text

class HasTable a e => InsertOrReplaceStatement a m e where
  type IoRStatement a e :: *
  insertOrReplace :: e -> a -> m (IoRStatement a e)

class HasTable a e => InsertStatement a m e where
  type InsStatement a e :: *
  insert :: e -> a -> m (InsStatement a e)

class ExistsStatement a m e where
  exists :: e -> a -> m Bool

class SelectStatement q m e where
  type SelectResult q :: *
  select :: e -> q -> m (SelectResult q)

class DeleteStatement q m e where
  type DeleteResult q :: *
  delete :: e -> q -> m (DeleteResult q)

class HasConnection e m where
  type DbConnection e :: *
  getConnection :: e -> m (DbConnection e)

class HasTransaction e m where
  transaction :: e -> m a -> m a

