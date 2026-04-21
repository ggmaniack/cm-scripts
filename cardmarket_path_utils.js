(function (root, factory) {
    const api = factory();

    if (typeof module === 'object' && module.exports) {
        module.exports = api;
    }

    root.cardmarketPathUtils = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    function normalizePath(input) {
        if (typeof input !== 'string') return '';
        return input.split(/[?#]/, 1)[0];
    }

    function parseCardmarketPath(pathname) {
        const normalizedPath = normalizePath(pathname);
        const segments = normalizedPath.split('/').filter(Boolean);
        if (segments.length < 2) return null;

        return {
            locale: segments[0],
            game: segments[1],
            segments: segments.slice(2)
        };
    }

    function isOffersPath(pathname) {
        const parsed = parseCardmarketPath(pathname);
        if (!parsed) return false;

        const [section, username, action] = parsed.segments;
        return section === 'Users' && Boolean(username) && action === 'Offers';
    }

    function isCartPath(pathname) {
        const parsed = parseCardmarketPath(pathname);
        if (!parsed) return false;

        return parsed.segments[0] === 'ShoppingCart';
    }

    function isProductPath(pathname) {
        const parsed = parseCardmarketPath(pathname);
        if (!parsed) return false;

        const [section, productType, category, product] = parsed.segments;
        return section === 'Products'
            && productType === 'Singles'
            && Boolean(category)
            && Boolean(product);
    }

    function toPathname(urlOrPath) {
        if (typeof urlOrPath !== 'string' || !urlOrPath) return '';

        try {
            return new URL(urlOrPath, 'https://www.cardmarket.com').pathname;
        } catch (error) {
            return normalizePath(urlOrPath);
        }
    }

    function isProductUrl(urlOrPath) {
        return isProductPath(toPathname(urlOrPath));
    }

    function isUserUrl(urlOrPath) {
        const parsed = parseCardmarketPath(toPathname(urlOrPath));
        if (!parsed) return false;

        const [section, username] = parsed.segments;
        return section === 'Users' && Boolean(username);
    }

    return {
        parseCardmarketPath,
        isOffersPath,
        isCartPath,
        isProductPath,
        isProductUrl,
        isUserUrl
    };
}));
