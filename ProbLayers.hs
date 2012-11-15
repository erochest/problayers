{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}

module Main where


import           Control.Applicative
import qualified Control.Monad as M
import qualified Data.Aeson as A
import qualified Data.Aeson.Types as AT
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import           Data.Int (Int64)
import           Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Database.Persist.GenericSql
import           Database.Persist.GenericSql.Raw
import           Database.Persist.Postgresql
import           Database.Persist.Store (PersistValue(..))
import qualified Filesystem.Path.CurrentOS as FS
import           Network.HTTP.Conduit
import           Network.HTTP.Conduit.Browser
import           Network.HTTP.Types (Method(..))
import           System.IO (writeFile)


-- | This represents a problem layer. It contains the database name and table
-- name.
data ProblemLayer = ProblemLayer
                  { problemDbName    :: T.Text
                  , problemTableName :: T.Text
                  }

-- | By default, ProblemLayers display as dot-separated.
instance Show ProblemLayer where
    show (ProblemLayer {..}) = 
        T.unpack $ problemDbName <> "." <> problemTableName

type AuthFn = Request (C.ResourceT IO) -> Request (C.ResourceT IO)

-- | This is the base URL for the GeoServer instance.
geoServerBaseUrl :: String
geoServerBaseUrl = "http://libsvr35.lib.virginia.edu:8080/geoserver/rest"

-- | This is the GeoServer authentication information.
geoServerAuth :: AuthFn
geoServerAuth = applyBasicAuth "slabadmin" "GIS4slab!"

-- | This is the base connection string for Postgres.
cxnString :: ConnectionString
cxnString = "host=lon.lib.virginia.edu user=err8n"

-- | This performs a REST request and returns the raw ByteString.
restBS :: String -> Method -> AuthFn -> IO BSL.ByteString
restBS url method authFn = do
    man  <- newManager def
    req  <- authFn <$> parseUrl url
    resp <- C.runResourceT
          . browse man
          . makeRequestLbs
          $ req { method = method }
    return $ responseBody resp

-- | This performs a REST request and parses the resulting JSON.
restJson :: AT.FromJSON a => String -> Method -> AuthFn -> IO (Maybe a)
restJson url method authFn = A.decode <$> restBS url method authFn

-- | This takes a directory name, a problem layer, and a GeoServer URL, and it
-- attempts to download the layer's data as XML from the source. Whatever it
-- can download, it saves in the output directory.
getProblemData :: String
               -> AuthFn
               -> FS.FilePath
               -> ProblemLayer
               -> IO FS.FilePath
getProblemData gsUrl authFn dirName (ProblemLayer {..}) =
    restBS url "GET" authFn >>=
    BSL.writeFile (FS.encodeString xml) >>
    return xml
    where url =  gsUrl ++ "/workspaces/" ++ T.unpack problemDbName
              ++ "/datastores/" ++ T.unpack problemDbName ++ ".json"
          xml = dirName FS.</> FS.fromText problemDbName FS.<.> "xml"

-- | This takes a single-item tuple containing text and returns the text.
getTextValue :: [PersistValue] -> Maybe T.Text
getTextValue [PersistText t] = Just t
getTextValue _               = Nothing

-- | This takes a single-item tuple containing an integer and returns the
-- number.
getIntValue :: [PersistValue] -> Maybe Int64
getIntValue [PersistInt64 i] = Just i
getIntValue _                = Nothing

-- | This gets the list of databases on the server.
getDbs :: ConnectionString -> IO [T.Text]
getDbs cxn = withPostgresqlConn cxn $ runSqlConn $ do
    let sql = "SELECT datname FROM pg_database WHERE datistemplate=false;"
    C.runResourceT $ withStmt sql []
        C.$= CL.mapMaybe getTextValue C.$$ CL.consume

-- | This gets the problem layers in the database.
getProblemLayers :: T.Text -> IO [ProblemLayer]
getProblemLayers dbName = withPostgresqlPool cxnString' 3 $ runSqlPool $ do
    isGis <- isGisDb
    if isGis
        then getMissingGisTables >>= M.mapM (return . ProblemLayer dbName)
        else return []
    where
        cxnString' = cxnString <> " dbname=" <> TE.encodeUtf8 dbName

        isGisDb = do
            let sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='geometry_columns';"
            output <- C.runResourceT $ withStmt sql []
                C.$= CL.mapMaybe getIntValue C.$= CL.map (== 1) C.$$ CL.take 1
            return (output == [True])

        getMissingGisTables = do
            let sql = "SELECT f_table_name FROM geometry_columns WHERE f_table_name NOT IN (SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema='public') ORDER BY f_table_name"
            C.runResourceT $ withStmt sql []
                C.$= CL.mapMaybe getTextValue C.$$ CL.consume

main :: IO ()
main =
    getDbs cxnString                                                >>=
    M.liftM concat . M.mapM getProblemLayers                        >>=
    M.mapM (getProblemData geoServerBaseUrl geoServerAuth "layers") >>=
    M.mapM_ print
