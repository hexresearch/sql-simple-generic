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

import Control.Monad.Catch hiding (Handler)
import Control.Newtype.Generics
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple hiding (In)
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToField (ToField(..))
import Data.ByteString (ByteString)
import Data.Data
import Data.Generics.Traversable
import Data.Generics.Traversable.Generic
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.String (IsString(..))
import Data.Text (Text)
import Data.Typeable
import GHC.Generics as Generics
import GHC.TypeLits
import qualified Database.PostgreSQL.Simple as PgSimple
import qualified Data.List as List
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

withEngine :: PostgreSQLEngineAttributes a => a -> (PostgreSQLEngine -> IO b) -> IO b
withEngine ea m = do
  e <- createEngine ea
  r <- m e
  disposeEngine e
  pure r

data Table (table :: Symbol) = From | Into | Table
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

data Update what values pred = Update what values pred

data All a = All

data Pred a = Eq a

newtype InSet a = InSet [a]
                  deriving (Data,Generic)

newtype Like a = Like Text
                 deriving (Show,Data,Generic)

-- FIXME: implement
data NVL a = NVL a
             deriving (Eq,Ord,Show,Data,Generic)

instance Newtype (NVL a)

newtype Bound a = Bound a

rows :: Rows a
rows = Rows

from :: Table t
from = From

into :: Table t
into = Into

table :: Table t
table = Table

values = Values

keyValues :: a -> b -> KeyValues a b
keyValues = KeyValues

returning :: Returning a
returning = Returning

class HasQParts t a where
  qparts :: a -> [QPart t]

instance {-# OVERLAPPABLE #-} (HasColumn t (Proxy a), HasBindValueList a, HasSQLOperator a) => HasQParts t a where
  qparts x = [QPart x]

instance (HasColumn t (Proxy a), HasBindValueList a, HasSQLOperator a) => HasQParts t (Maybe a) where
  qparts = fmap QPart . maybeToList

gMakeClauses :: forall t a . (GTraversable (HasQParts t) a) => a -> [QPart t]
gMakeClauses = gfoldMap @(HasQParts t) (qparts @t)

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
--     print sql
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
         , HasColumns (ColumnSet t values)
         , HasColumns (QueryPart t pred)
         , HasBindValueList ()
         , HasBindValueList pred
         , ToRow values
         , values ~ ()
         ) => UpdateStatement (Update (Table t) (Values ()) (Where pred)) IO PostgreSQLEngine where
  type UpdateResult (Update (Table t) (Values ()) (Where pred)) = Integer
  update eng _ = pure 0

instance ( KnownSymbol t
         , HasColumns (ColumnSet t values)
         , HasColumns (QueryPart t pred)
         , HasBindValueList values
         , HasBindValueList pred
         , ToRow values
         ) => UpdateStatement (Update (Table t) (Values values) (Where pred)) IO PostgreSQLEngine where
  type UpdateResult (Update (Table t) (Values values) (Where pred)) = Integer
  update eng (Update _ (Values vals) (Where pred)) = do
    conn <- getConnection eng
    let sql = [qc|update {table} set {setDecl} where {whereCols}|]
    print sql
    n <- execute conn sql (bindVals <> bindPred)
    pure (fromIntegral n)

    where
      table = tablename eng (Proxy @(Table t))

      bindVals = bindValueList vals
      bindPred = bindValueList pred

      setDecl :: Text
      setDecl | List.null setDecl' = ""
              | otherwise = Text.intercalate "," setDecl'

      setDecl' = [ [qc|{col} = ?|] | col <- columns (ColumnSet :: ColumnSet t values) ]

      whereCols :: Text.Text
      whereCols | List.null bindPred = "true"
                | otherwise  =
        Text.intercalate " and " (columns (QueryPart @t pred))


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

class HasProxy a where
  proxyOf :: a -> Proxy a

instance HasProxy a where
  proxyOf = const $ Proxy @a

data QPart (t::Symbol) = forall a . ( HasProxy a
                                    , HasColumn t (Proxy a)
                                    , HasSQLOperator a
                                    , HasBindValueList a) => QPart a

clause  :: forall t a . ( HasProxy a
                      , HasColumn t (Proxy a)
                      , HasBindValueList a
                      , HasSQLOperator a
                      ) => a -> QPart t
clause x = QPart x

like :: forall t a . ( HasProxy a
                     , ToField a
                     , HasColumn t (Proxy a)
                     , HasBindValueList a
                     , HasSQLOperator a
                     ) => Text -> QPart t
like x = QPart (Like @a x)

data ToRowItem = forall a . ToField a => ToRowItem a

instance ToField ToRowItem where
  toField (ToRowItem x) = toField x

class GenericBindValueList a

class HasBindValueList a where
  bindValueList :: a -> [ToRowItem]

instance {-# OVERLAPPABLE #-} GHasBindVals a (Rep a) => HasBindValueList a where
  bindValueList x = gbinds @a @(Rep a) x

instance HasBindValueList (QPart t) where
  bindValueList (QPart x) = bindValueList x

instance HasBindValueList [QPart t] where
  bindValueList a = foldMap bindValueList a

instance HasBindValueList () where
  bindValueList = const mempty


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

class HasSQLOperator a where
  sqlOperator :: Proxy a -> Text

instance {-# OVERLAPPABLE #-} HasSQLOperator a where
  sqlOperator = const " = ?"

instance HasSQLOperator (Like a) where
  sqlOperator = const " like ?"

instance HasColumn t (Proxy a) => HasColumn t (Proxy (Like a)) where
  column pt _ = column pt (Proxy @a)

instance HasColumn t (Proxy a) => HasColumn t (Proxy (NVL a)) where
  column pt _ = column pt (Proxy @a)

instance ToField a => ToField (Like a) where
  toField (Like x) = toField x

instance HasSQLOperator (InSet a) where
  sqlOperator = const " in ?"

instance (KnownSymbol t, HasColumn t (Proxy a)) => HasColumn t (Proxy (Maybe a)) where
  column pt _ = column pt (Proxy @a)

instance (KnownSymbol t, HasColumn t (Proxy a)) => HasColumn t (Like a) where
  column pt _ = column pt (Proxy @a)

instance {-# OVERLAPPABLE #-} (KnownSymbol t, GHasCols t a (Rep a)) => HasColumns (QueryPart t a) where
  columns (QueryPart x) =  gcols @t @a @(Rep a) x

class GHasBindVals a (f :: * -> *) where
  gbinds :: a -> [ToRowItem]

instance ToField a => GHasBindVals a (M1 D ('MetaData i k l 'True) f) where
  gbinds a = [ToRowItem a]

instance (GTraversable HasBindValueList a) => GHasBindVals a (M1 D ('MetaData i k l 'False) f) where
  gbinds = gfoldMap @HasBindValueList bindValueList

class GHasCols t a (f :: * -> *)  where
  gcols :: a -> [Text]

instance (HasColumn t (Proxy a)) => GHasCols t a (M1 D ('MetaData i k l 'True) f) where
  gcols _ = [ exprOf (column (Proxy @t) (Proxy @a)) (sqlOperator (Proxy @a)) ]
    where
      exprOf :: Text -> Text -> Text
      exprOf col op = [qc| {col} {op} |]


instance (GTraversable (HasQParts t) a) => GHasCols t a (M1 D ('MetaData i k l 'False) f) where
  gcols x = fmap mkColumn (gMakeClauses @t x)
    where
      mkColumn (QPart e) = [qc| {col} {op} |]
        where col = column (Proxy @t) (proxyOf e)
              op  = sqlOperator (proxyOf e)


instance HasColumns (QueryPart t [QPart t]) where
  columns = const mempty

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

