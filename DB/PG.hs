{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeInType #-}
module DB.PG ( module DB
             , module DB.PG
             ) where

import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple hiding (In)
import Database.PostgreSQL.Simple.ToField
import Data.ByteString (ByteString)
import Control.Monad.Catch hiding (Handler)
import Data.Int
import Data.Proxy
import Data.String (IsString(..))
import Data.Text (Text)
import GHC.TypeLits
import GHC.Generics as Generics
import qualified Database.PostgreSQL.Simple as PgSimple
import qualified Data.List as List
import qualified Data.Text as Text
import Text.InterpolatedString.Perl6 (qc)
import Data.Typeable

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

withEngine :: PostgreSQLEngineAttributes a => a -> (PostgreSQLEngine -> IO b) -> IO b
withEngine ea m = do
  e <- createEngine ea
  r <- m e
  disposeEngine e
  pure r

data Table (table :: Symbol) = From | Into
data Rows cols = Rows

data QueryPart (table :: Symbol) pred = QueryPart pred

data ColumnSet (table :: Symbol) cols = ColumnSet

data Where p = Where p

data Values p = Values p

data KeyValues k v = KeyValues k v

data Returning a = Returning

data Select what from pred = Select what from (Where pred)

data Insert into values ret = Insert into values ret

data Delete what pred = Delete what pred

data All a = All

data Pred a = Eq a

newtype InSet a = InSet [a]

newtype Bound a = Bound a

rows :: Rows a
rows = Rows

from :: Table t
from = From

into :: Table t
into = Into

values = Values

keyValues :: a -> b -> KeyValues a b
keyValues = KeyValues

returning :: Returning a
returning = Returning


instance FromRow () where
  fromRow = (field :: (RowParser (Maybe Int)) ) >> pure ()

instance KnownSymbol t => HasTable (Proxy (Table t)) e where
  tablename _ _ = fromString (symbolVal (Proxy @t))

instance ( KnownSymbol t
         , HasColumns (ColumnSet t row)
         , HasColumns (QueryPart t pred)
         , FromRow row
         , HasBindValueList pred
         ) => SelectStatement (Select (Rows [row]) (Table t) pred) IO PostgreSQLEngine where
  type SelectResult (Select (Rows [row]) (Table t) pred) = [row]
  select eng (Select _ _ (Where foo)) = do
    conn <- getConnection eng
    let sql = [qc|select {cols} from {table} where {whereCols}|]
    print sql
    query conn sql binds
    where
      binds = bindValueList foo
      table = tablename eng (Proxy @(Table t))
      cols  = Text.intercalate "," (columns (ColumnSet :: ColumnSet t row))
      whereCols :: Text.Text
      whereCols | List.null binds = "true"
                | otherwise  =
        Text.intercalate " and " (columns (QueryPart @t foo))


instance ( KnownSymbol t
         , HasColumns (QueryPart t pred)
         , HasBindValueList pred
         ) => DeleteStatement (Delete (Table t) (Where pred)) IO PostgreSQLEngine where
  type DeleteResult (Delete (Table t) (Where pred)) = Int64
  delete eng (Delete _ (Where foo)) = do
    conn <- getConnection eng
    let sql = [qc|delete from {table} where {whereCols}|]
--     print sql
    execute conn sql binds
    where
      binds = bindValueList foo
      table = tablename eng (Proxy @(Table t))
      whereCols :: Text.Text
      whereCols | List.null binds = "true"
                | otherwise  =
        Text.intercalate " and " (columns (QueryPart @t foo))

instance ( KnownSymbol t
         , HasColumn t (Proxy a)
         ) => HasColumn t (Proxy (InSet a)) where
  column _ _ = column (Proxy @t) (Proxy @a)


instance ToField a => ToField (InSet a) where
  toField (InSet x) = toField (PgSimple.In x)

instance KnownSymbol t => HasTable (Insert (Table t) values r) e where
  tablename e _ = tablename e (Proxy @(Table t))

instance ( KnownSymbol t
         , HasColumns (ColumnSet t ret)
         , HasColumns (ColumnSet t values)
         , FromRow ret
         , ToRow values
         , FromRow ret
         ) => InsertStatement (Insert (Table t) (Values values) (Returning ret)) IO PostgreSQLEngine where
  type InsStatement (Insert (Table t) (Values values) (Returning ret)) PostgreSQLEngine = [ret]
  insert eng st@(Insert _ (Values values) _) = do
    conn <- getConnection eng
    let sql = [qc|insert into {table} {colDef} {valDef} returning {retColDef}|]
--     print sql
    query conn sql values
    where
      table = tablename eng st
      retColNames = columns (ColumnSet :: ColumnSet t ret)
      colNames = columns (ColumnSet :: ColumnSet t values)
      cols     = Text.intercalate "," colNames
      retCols  = Text.intercalate "," retColNames
      binds    = Text.intercalate "," [ "?" | x <- colNames ]

      retColDef :: Text
      retColDef = case retColNames of
        [] -> "null"
        _  -> retCols

      colDef :: Text
      colDef = case colNames of
        [] -> ""
        _  -> [qc|({cols})|]

      valDef :: Text
      valDef = case colNames of
        [] -> "default values"
        _  -> [qc|values({binds})|]

insertB :: (InsertStatement a m e, MonadCatch m)  => e -> a -> m Bool
insertB eng statement =
    handle
      ( \case
          -- 23505: "duplicate key value violates unique constraint"
          (PgSimple.SqlError {sqlState = "23505"}) -> pure False
          e -> throwM e
      ) $
      fmap (const True) $ insert eng statement

data ToRowItem = forall a . ToField a => ToRowItem a

instance ToField ToRowItem where
  toField (ToRowItem x) = toField x

class HasBindValueList a where
  bindValueList :: a -> [ToRowItem]

instance {-# OVERLAPPABLE #-}  ToField a => HasBindValueList a where
  bindValueList a = [ToRowItem a]

instance HasBindValueList () where
  bindValueList = const mempty

instance ( ToField a1
         , ToField a2
         ) => HasBindValueList (a1,a2) where
  bindValueList (a1,a2) = bindValueList a1 <> bindValueList a2

instance ( ToField a1
         , ToField a2
         , ToField a3
         ) => HasBindValueList (a1,a2,a3) where
  bindValueList (a1,a2,a3) =    bindValueList a1
                             <> bindValueList a2
                             <> bindValueList a3

instance ( ToField a1
         , ToField a2
         , ToField a3
         , ToField a4
         ) => HasBindValueList (a1,a2,a3,a4) where
  bindValueList (a1,a2,a3,a4) =    bindValueList a1
                                <> bindValueList a2
                                <> bindValueList a3
                                <> bindValueList a4

instance ( ToField a1
         , ToField a2
         , ToField a3
         , ToField a4
         , ToField a5
         ) => HasBindValueList (a1,a2,a3,a4,a5) where
  bindValueList (a1,a2,a3,a4,a5) =   bindValueList a1
                                  <> bindValueList a2
                                  <> bindValueList a3
                                  <> bindValueList a4
                                  <> bindValueList a5

instance (HasBindValueList a, HasBindValueList b) => HasBindValueList (KeyValues a b) where
  bindValueList (KeyValues a b) = bindValueList a <> bindValueList b

instance (HasBindValueList a) => HasBindValueList (Where a) where
  bindValueList (Where x) = bindValueList x

instance ( KnownSymbol t
         , HasColumns (ColumnSet t ret)
         , HasColumns (ColumnSet t k)
         , HasColumns (ColumnSet t v)
         , HasBindValueList (KeyValues k v)
         , FromRow ret
         ) =>
  InsertOrReplaceStatement (Insert (Table t) (KeyValues k v) (Returning ret)) IO PostgreSQLEngine where
  type IoRStatement (Insert (Table t) (KeyValues k v ) (Returning ret)) PostgreSQLEngine = [ret]
  insertOrReplace eng st@(Insert _ (KeyValues k v) _) = do
    conn <- getConnection eng
--     print sql
    query conn sql (bindValueList (KeyValues k v))
    where
      sql = [qc|
insert into {table} {colDef} {valDef}
{onconflict}
returning {retColDef}
|]
      table = tablename eng st

      colNames = colsK <> colsV

      colDef :: Text
      colDef   = case colsList colNames of
                   "" -> ""
                   x  -> [qc|({x})|]

      onconflict :: Text
      onconflict = case (colsK, colsV) of
        ((x:xs),[])     -> [qc|on conflict ({colsList colsK}) do nothing|]
        ((x:[]),ys)     -> [qc|on conflict ({x}) do update set {updVals}|]
        ((x:xs),(y:ys)) -> [qc|on conflict ({colsList colsK}) do update set {updVals}|]
        (_,_)           -> ""

      colsList = Text.intercalate ","

      updVals = colsList [ [qc|{v} = excluded.{v}|] | v <- colsV ]

      retColNames = columns (ColumnSet :: ColumnSet t ret)

      colsK  = columns (ColumnSet :: ColumnSet t k)
      colsV =  columns (ColumnSet :: ColumnSet t v)

      retCols  = colsList retColNames
      binds    = colsList [ "?" | x <- colNames ]

      retColDef :: Text
      retColDef = case retColNames of
        [] -> "null"
        _  -> retCols

      valDef :: Text
      valDef = case colNames of
        [] -> "default values"
        _  -> [qc|values({binds})|]


instance {-# OVERLAPPABLE #-}
         ( KnownSymbol t
         , Generic a
         , GColProxy t a (Rep a)
         ) => HasColumns (ColumnSet t a) where
  columns _ = fmap (\(ColumnProxy t c) -> column t c) (colproxy @t @a)

instance {-# OVERLAPPING #-} (KnownSymbol t) => HasColumns (ColumnSet t ()) where
  columns = const []


data ColumnProxy (t::Symbol) = forall a . (HasColumn t (Proxy a)) => ColumnProxy (Proxy t) (Proxy a)

instance {-# OVERLAPPABLE #-}(KnownSymbol t, HasColumn t (Proxy a)) => HasColumns (QueryPart t a) where
  columns _ = [ exprOf $ column (Proxy @t) (Proxy @a) ]
    where
      exprOf :: Text -> Text
      exprOf x = [qc|{x} = ?|]

instance (KnownSymbol t, HasColumn t (Proxy a)) => HasColumns (QueryPart t (InSet a)) where
  columns _ = [ [qc|{col} in ?|]  ]
    where col = column (Proxy @t) (Proxy @a)

instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2))
      => HasColumns (QueryPart t (a1,a2)) where
  columns (QueryPart (x1,x2)) = columns (QueryPart @t x1 ) <> columns (QueryPart @t x2)


instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3))
      => HasColumns (QueryPart t (a1,a2,a3)) where
  columns (QueryPart (x1,x2,x3)) =  columns (QueryPart @t x1 )
                                 <> columns (QueryPart @t x2)
                                 <> columns (QueryPart @t x3)


instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3)
         , HasColumns (QueryPart t a4)
         )
      => HasColumns (QueryPart t (a1,a2,a3,a4)) where
  columns (QueryPart (x1,x2,x3,x4)) =  columns (QueryPart @t x1)
                                    <> columns (QueryPart @t x2)
                                    <> columns (QueryPart @t x3)
                                    <> columns (QueryPart @t x4)

instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3)
         , HasColumns (QueryPart t a4)
         , HasColumns (QueryPart t a5)
         )
      => HasColumns (QueryPart t (a1,a2,a3,a4,a5)) where
  columns (QueryPart (x1,x2,x3,x4,x5)) =  columns (QueryPart @t x1)
                                      <> columns (QueryPart @t x2)
                                      <> columns (QueryPart @t x3)
                                      <> columns (QueryPart @t x4)
                                      <> columns (QueryPart @t x5)


instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3)
         , HasColumns (QueryPart t a4)
         , HasColumns (QueryPart t a5)
         , HasColumns (QueryPart t a6)
         )
      => HasColumns (QueryPart t (a1,a2,a3,a4,a5,a6)) where
  columns (QueryPart (x1,x2,x3,x4,x5,x6)) =  columns (QueryPart @t x1)
                                         <> columns (QueryPart @t x2)
                                         <> columns (QueryPart @t x3)
                                         <> columns (QueryPart @t x4)
                                         <> columns (QueryPart @t x5)
                                         <> columns (QueryPart @t x6)


instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3)
         , HasColumns (QueryPart t a4)
         , HasColumns (QueryPart t a5)
         , HasColumns (QueryPart t a6)
         , HasColumns (QueryPart t a7)
         )
      => HasColumns (QueryPart t (a1,a2,a3,a4,a5,a6,a7)) where
  columns (QueryPart (x1,x2,x3,x4,x5,x6,x7)) =  columns (QueryPart @t x1)
                                         <> columns (QueryPart @t x2)
                                         <> columns (QueryPart @t x3)
                                         <> columns (QueryPart @t x4)
                                         <> columns (QueryPart @t x5)
                                         <> columns (QueryPart @t x6)
                                         <> columns (QueryPart @t x7)

instance ( KnownSymbol t
         , HasColumns (QueryPart t a1)
         , HasColumns (QueryPart t a2)
         , HasColumns (QueryPart t a3)
         , HasColumns (QueryPart t a4)
         , HasColumns (QueryPart t a5)
         , HasColumns (QueryPart t a6)
         , HasColumns (QueryPart t a7)
         , HasColumns (QueryPart t a8)
         )
      => HasColumns (QueryPart t (a1,a2,a3,a4,a5,a6,a7,a8)) where
  columns (QueryPart (x1,x2,x3,x4,x5,x6,x7,x8)) =  columns (QueryPart @t x1)
                                         <> columns (QueryPart @t x2)
                                         <> columns (QueryPart @t x3)
                                         <> columns (QueryPart @t x4)
                                         <> columns (QueryPart @t x5)
                                         <> columns (QueryPart @t x6)
                                         <> columns (QueryPart @t x7)
                                         <> columns (QueryPart @t x8)



instance HasColumns (QueryPart t ()) where
  columns = const mempty

colproxy :: forall t  a . (KnownSymbol t, Generic a, GColProxy t a (Rep a)) => [ColumnProxy t]
colproxy = gcolproxy @t @a @(Rep a)

class GColProxy t a (f :: * -> *) where
  gcolproxy :: [ColumnProxy t]

instance (HasColumn t (Proxy a)) => GColProxy t a (M1 D ('MetaData i k l 'True) f) where
  gcolproxy = [ ColumnProxy (Proxy @t) (Proxy @a) ]

instance (GColProxy' t f) => GColProxy t a (M1 D ('MetaData i k l 'False) f) where
  gcolproxy = gcolproxy' @t @f

class GColProxy' t (f :: * -> *) where
  gcolproxy' :: [ColumnProxy t]

instance GColProxy' t f => GColProxy' t (M1 c m f) where
  gcolproxy' = gcolproxy' @t @f

instance (GColProxy' t f, GColProxy' t g) => GColProxy' t (f :*: g) where
  gcolproxy' = gcolproxy' @t @f <> gcolproxy' @t @g

instance (HasColumn t (Proxy a)) => GColProxy' t (K1 r a) where
  gcolproxy' = [ ColumnProxy (Proxy @t) (Proxy @a) ]

instance (HasColumn t (Proxy a)) => GColProxy' t U1 where
  gcolproxy' = mempty


