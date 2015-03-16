{-# LANGUAGE OverloadedStrings #-}

import           Hakyll
import           System.FilePath

main :: IO ()
main = hakyll $ do
  match "trough/images/**" $ do
    route   idRoute
    compile copyFileCompiler

  match "trough/css/**" $ do
    route   idRoute
    compile copyFileCompiler

  match "index.md" $ do
    route   $ setExtension "html"
    compile $ pandocCompiler
      >>= loadAndApplyTemplate "templates/default.html" defaultContext
      >>= relativizeUrls

  match "src/*.md" $ do
    route niceRoute
    compile $ pandocCompiler
      >>= loadAndApplyTemplate "templates/default.html" defaultContext
      >>= relativizeUrls

  match "templates/*" $ compile templateCompiler

-- From: http://yannesposito.com/Scratch/en/blog/Hakyll-setup/
niceRoute :: Routes
niceRoute = customRoute $ \ident ->
    let p = toFilePath ident
     in takeDirectory p </> takeBaseName p </> "index.html"
