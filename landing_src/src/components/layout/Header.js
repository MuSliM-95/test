import React, { useState, useRef, useEffect } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import Logo from '../../assets/images/logo.svg';

const propTypes = {
  navPosition: PropTypes.string,
  hideNav: PropTypes.bool,
  hideSignin: PropTypes.bool,
  bottomOuterDivider: PropTypes.bool,
  bottomDivider: PropTypes.bool
}

const defaultProps = {
  navPosition: '',
  hideNav: false,
  hideSignin: false,
  bottomOuterDivider: false,
  bottomDivider: false
}

const Header = ({
  className,
  navPosition,
  hideNav,
  hideSignin,
  bottomOuterDivider,
  bottomDivider,
  ...props
}) => {

  const [isActive, setIsactive] = useState(false);

  const nav = useRef(null);
  const hamburger = useRef(null);

  useEffect(() => {
    isActive && openMenu();
    document.addEventListener('keydown', keyPress);
    document.addEventListener('click', clickOutside);
    return () => {
      document.removeEventListener('keydown', keyPress);
      document.removeEventListener('click', clickOutside);
      closeMenu();
    };
  });  

  const openMenu = () => {
    document.body.classList.add('off-nav-is-active');
    nav.current.style.maxHeight = nav.current.scrollHeight + 'px';
    setIsactive(true);
  }

  const closeMenu = () => {
    document.body.classList.remove('off-nav-is-active');
    nav.current && (nav.current.style.maxHeight = null);
    setIsactive(false);
  }

  const keyPress = (e) => {
    isActive && e.keyCode === 27 && closeMenu();
  }

  const clickOutside = (e) => {
    if (!nav.current) return
    if (!isActive || nav.current.contains(e.target) || e.target === hamburger.current) return;
    closeMenu();
  }  

  const classes = classNames(
    'site-header',
    bottomOuterDivider && 'has-bottom-divider',
    className
  );

  return (
    <header
      {...props}
      className={classes}
    >
      <div className="container">
        <div className={
          classNames(
            'site-header-inner',
            bottomDivider && 'has-bottom-divider'
          )}>
          <img alt={"logo_image"} src={Logo} />
          {!hideNav &&
            <>
              <button
                ref={hamburger}
                className="header-nav-toggle"
                onClick={isActive ? closeMenu : openMenu}
              >
                <span className="screen-reader">Menu</span>
                <span className="hamburger">
                  <span className="hamburger-inner"></span>
                </span>
              </button>
              <nav
                ref={nav}
                className={
                  classNames(
                    'header-nav',
                    isActive && 'is-active'
                  )}>
                <div className="header-nav-inner">
                  <ul className={
                    classNames(
                      'list-reset text-xs',
                      navPosition && `header-nav-${navPosition}`
                    )}>
                    <li>
                      <a href={`https://${process.env.REACT_APP_APP_URL}/api/v1/docs`}>Документация</a>
                    </li>
                  </ul>
                  {!hideSignin &&
                    <ul
                      className="list-reset header-nav-right"
                    >
                      <li>
                        <a href="https://t.me/tablecrmbot" className="button button-primary button-wide-mobile button-sm">Зарегистрироваться</a>
                      </li>
                    </ul>}
                </div>
              </nav>
            </>}
        </div>
      </div>
    </header>
  );
}

Header.propTypes = propTypes;
Header.defaultProps = defaultProps;

export default Header;


















// import React, { useState, useRef, useEffect } from 'react';
// import PropTypes from 'prop-types';
// import classNames from 'classnames';
// import Button from '../elements/Button';

// import {Link} from 'react-router-dom';

// import Logo from '../../assets/images/logo.svg';

// const propTypes = {
//   navPosition: PropTypes.string,
//   hideNav: PropTypes.bool,
//   hideSignin: PropTypes.bool,
//   bottomOuterDivider: PropTypes.bool,
//   bottomDivider: PropTypes.bool
// }

// const defaultProps = {
//   navPosition: '',
//   hideNav: false,
//   hideSignin: false,
//   bottomOuterDivider: false,
//   bottomDivider: false
// }

// const Header = ({
//   className,
//   navPosition,
//   hideNav,
//   hideSignin,
//   bottomOuterDivider,
//   bottomDivider,
//   ...props
// }) => {

//   const [isActive, setIsactive] = useState(false);

//   const nav = useRef(null);
//   const hamburger = useRef(null);

//   useEffect(() => {
//     isActive && openMenu();
//     document.addEventListener('keydown', keyPress);
//     document.addEventListener('click', clickOutside);
//     return () => {
//       document.removeEventListener('keydown', keyPress);
//       document.removeEventListener('click', clickOutside);
//       closeMenu();
//     };
//   });  

//   const openMenu = () => {
//     document.body.classList.add('off-nav-is-active');
//     nav.current.style.maxHeight = nav.current.scrollHeight + 'px';
//     setIsactive(true);
//   }

//   const closeMenu = () => {
//     document.body.classList.remove('off-nav-is-active');
//     nav.current && (nav.current.style.maxHeight = null);
//     setIsactive(false);
//   }

//   const keyPress = (e) => {
//     isActive && e.keyCode === 27 && closeMenu();
//   }

//   const clickOutside = (e) => {
//     if (!nav.current) return
//     if (!isActive || nav.current.contains(e.target) || e.target === hamburger.current) return;
//     closeMenu();
//   }  

//   const classes = classNames(
//     'site-header',
//     bottomOuterDivider && 'has-bottom-divider',
//     className
//   );

//   return (
//     <header
//     {...props}
//     className={classes}
//   >
//     <div className="container">
//       <div className={
//         classNames(
//           'site-header-inner',
//           bottomDivider && 'has-bottom-divider'
//         )}>
//          <img src={Logo} />
//         {!hideNav &&
//           <>
//             <button
//               ref={hamburger}
//               className="header-nav-toggle"
//               onClick={isActive ? closeMenu : openMenu}
//             >
//               <span className="screen-reader">Menu</span>
//               <span className="hamburger">
//                 <span className="hamburger-inner"></span>
//               </span>
//             </button>
//             <nav
//               ref={nav}
//               className={
//                 classNames(
//                   'header-nav',
//                   isActive && 'is-active'
//                 )}>

//               <Button href="https://t.me/tablecrmbot" 
//               className="button button-primary button-wide-mobile button-sm motger" 
//               onClick={closeMenu}>Начать работу</Button>
//               <div className="header-nav-inner">
//                 <ul className={
//                   classNames(
//                     'list-reset text-xs',
//                     navPosition && `header-nav-${navPosition}`
//                   )}>
//                 </ul>
//               </div>
//             </nav>
//           </>}
//       </div>
//     </div>
//   </header>
//   );
// }

// Header.propTypes = propTypes;
// Header.defaultProps = defaultProps;

// export default Header;
