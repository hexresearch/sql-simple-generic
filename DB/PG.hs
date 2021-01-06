{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
{-# LANGUAGE UndecidableInstances #-}
module DB.PG ( module DB
             , module DB.PG
             ) where

import Database.PostgreSQL.Simple
import Data.ByteString (ByteString)
import Data.Proxy
import Data.String (IsString(..))
import Data.Text (Text)
import GHC.TypeLits
import qualified Data.Text as Text
import Text.InterpolatedString.Perl6 (qc)

import DB

class PostgreSQLEngineAttributes a where
  connectionString :: a -> ByteString

instance PostgreSQLEngineAttributes String where
  connectionString = fromString

-- DAL/PG
newtype PostgreSQLEngine = PostgreSQLEngine { conn :: Connection }

instance Monad m => HasConnection PostgreSQLEngine m where
  type DbConnection PostgreSQLEngine = Connection
  getConnection eng = pure $ conn eng

instance HasTransaction PostgreSQLEngine IO where
  transaction eng action = do
    conn <- getConnection eng
    withTransaction conn action


createEngine :: PostgreSQLEngineAttributes a => a -> IO PostgreSQLEngine
createEngine att = PostgreSQLEngine <$> connectPostgreSQL (connectionString att)

disposeEngine :: PostgreSQLEngine -> IO ()
disposeEngine e = do
  conn <- getConnection e
  close conn

data Table (table :: Symbol) = From
data Rows cols = Rows

data RecordSet (table :: Symbol) cols = RecordSet

data QueryPart (table :: Symbol) pred = QueryPart pred

data Where p = Where p

data Select what from pred = Select what from (Where pred)

data All a = All

data Pred a = Eq a

newtype Bound a = Bound a

rows :: Rows a
rows = Rows

from :: Table t
from = From

class SQLQueryPart t p where
  sqlFrom :: QueryPart t p -> Text

class HasBoundValues a e where
  type BoundValues a e :: *
  boundValues :: e -> a -> BoundValues a e

instance KnownSymbol t => HasTable (RecordSet t cols) e where
  tablename _ _ = fromString (symbolVal (Proxy @t))

instance KnownSymbol t => HasTable (Proxy (Table t)) e where
  tablename _ _ = fromString (symbolVal (Proxy @t))

instance ( KnownSymbol t
         , HasTable   (RecordSet t [row]) PostgreSQLEngine
         , HasColumns (RecordSet t [row])
         , FromRow row
         ) => SelectStatement (All (RecordSet t [row])) IO PostgreSQLEngine
      where
        type SelectResult (All (RecordSet t [row])) = [row]
        select eng q = do
          conn <- getConnection eng
          query_ conn [qc|select {cols} from {table}|]
          where table = tablename eng q
                cols  = Text.intercalate "," (columns (RecordSet :: RecordSet t [row]))

instance ( KnownSymbol t
         , HasColumns (RecordSet t [row])
         , HasColumns (QueryPart t (Where pred))
         , FromRow row
         , HasBoundValues (Bound pred) PostgreSQLEngine
         , ToRow (BoundValues (Bound pred) PostgreSQLEngine)
         ) => SelectStatement (Select (Rows [row]) (Table t) pred) IO PostgreSQLEngine where
  type SelectResult (Select (Rows [row]) (Table t) pred) = [row]
  select eng (Select _ _ (Where p)) = do
    conn <- getConnection eng
    let sql = [qc|select {cols} from {table} where {whereCols};|]
    print sql
    query conn sql (boundValues eng (Bound p))
    where
      table = tablename eng (Proxy @(Table t))
      cols  = Text.intercalate "," (columns (RecordSet :: RecordSet t [row]))
      whereCols :: Text.Text
      whereCols =
        Text.intercalate " and " [  [qc|{c} = ?|] | c <- columns (QueryPart @t (Where p)) ]

instance (KnownSymbol t, HasColumn t (Proxy p)) => HasColumns (QueryPart t (Where p)) where
  columns = const [ column (Proxy @t) (Proxy @p) ]

instance KnownSymbol t => HasTable (All (RecordSet t a)) e where
  tablename _ _ = fromString $ symbolVal (Proxy @t)

instance ( HasColumn t (Proxy a)
         , KnownSymbol t
         ) => HasColumns (QueryPart t [(a,b)])   where
  columns = const [ column (Proxy @t) (Proxy @a)
                  ]

instance ( HasColumn t (Proxy a)
         , HasColumn t (Proxy b)
         , KnownSymbol t
         ) => HasColumns (RecordSet t [(a, b)]) where
  columns = const [ column (Proxy @t) (Proxy @a)
                  , column (Proxy @t) (Proxy @b)
                  ]

instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , KnownSymbol t
         ) => HasColumns (RecordSet t [(a1, a2, a3)]) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , HasColumn t (Proxy a4)
         , KnownSymbol t
         ) => HasColumns (RecordSet t [(a1, a2, a3, a4)]) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  , column (Proxy @t) (Proxy @a4)
                  ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , HasColumn t (Proxy a4)
         , HasColumn t (Proxy a5)
         , KnownSymbol t
         ) => HasColumns (RecordSet t [(a1, a2, a3, a4, a5)]) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  , column (Proxy @t) (Proxy @a4)
                  , column (Proxy @t) (Proxy @a5)
                  ]


instance HasBoundValues (Bound a) PostgreSQLEngine where
  type BoundValues (Bound a) PostgreSQLEngine = Only a
  boundValues _ (Bound x) = Only x

