{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module DB.PG.Instances where

import Control.Newtype as Newtype
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Data.Proxy
import Data.Scientific
import Data.String
import Data.Text (Text)
import GHC.TypeLits (KnownSymbol(..), symbolVal)
import qualified Data.Text as Text
import qualified GHC.TypeLits as T
import Text.InterpolatedString.Perl6 (qc)

import DB.PG
import Data.MOEX

instance FromRow Symbol where
  fromRow = Symbol <$> field

instance ToField Symbol where
  toField x = toField (Newtype.unpack x)

instance FromField Symbol where
  fromField f x = Symbol <$> fromField f x

instance FromField BondShortName where
  fromField f x = BondShortName <$> fromField f x

instance FromField BondFullName where
  fromField f x = BondFullName <$> fromField f x

instance ToField Market where
  toField (Market x) = toField x

instance ToField Price where
  toField (Price x) = toField (realToFrac x :: Scientific)

instance ToField Volume where
  toField (Volume x) = toField (realToFrac x :: Scientific)

data From (table :: T.Symbol) cols = All
data Rows cols

type family SelectRowType a :: *

type instance SelectRowType (From t (Rows [row])) = row

instance ( HasTable q PostgreSQLEngine
         , HasColumns q
         , FromRow a
         , SelectRowType q ~ a
         ) => SelectStatement [a] q IO PostgreSQLEngine
      where
        select eng q = do
          conn <- getConnection eng
          query_ conn [qc|select {cols} from {table}|]
          where table = tablename eng q
                cols  = Text.intercalate "," (columns q)

instance ( HasColumn t (Proxy a)
         , HasColumn t (Proxy b)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a, b)])) where
  columns = const [ column (Proxy @t) (Proxy @a), column (Proxy @t) (Proxy @b) ]


instance KnownSymbol t => HasTable (From t cols) e where
  tablename _ _ = fromString (symbolVal (Proxy @t))

instance HasColumn "bondshortname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondshortname" (Proxy BondShortName) where
  column _ _ = "value"

instance HasColumn "bondfullname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondfullname" (Proxy BondFullName) where
  column _ _ = "value"


